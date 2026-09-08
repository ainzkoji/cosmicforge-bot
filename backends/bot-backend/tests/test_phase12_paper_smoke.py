"""Phase 12 — clean paper runtime smoke validation.

This is not a profitability test. It proves plumbing:

    data -> decision -> risk -> execution -> lifecycle -> persistence
         -> restart -> close

Ground rules taken from the blueprint:

* A **fresh** validation bot. Never ``bot_e5fe913972a9`` or ``bot_a8117dc719fc``
  — those are the subject of an unresolved forensic finding and are not touched.
* Provenance is ``PAPER_FORWARD_VALIDATION``, which is deliberately outside
  ORGANIC_PROVENANCE, so nothing here can inflate readiness evidence.
* Execution terminates in ``PaperExecutor``. No broker order API is reachable.
* Production thresholds are NOT lowered. The controlled approved opportunity is
  injected below strategy-quality generation and above risk/execution, so the
  real TradingDecisionEngine, risk, feasibility, EntryProtection and executor
  all still run.
"""
from __future__ import annotations

import pytest

from app.decision import TradingDecisionEngine
from app.decision.opportunity import build_opportunity
from app.decision.reasons import CycleReason, QualityReason
from app.evidence.decision_recorder import record_decision, record_no_new_candle
from app.evidence.integrity import run_integrity_checks
from app.evidence.writers import (
    complete_execution_attempt,
    open_bot_run,
    open_execution_attempt,
    open_runtime_session,
    record_bot_lifecycle_event,
    record_market_snapshot,
    record_position_event,
    record_position_opened,
    record_trading_cycle,
    update_position_quantities,
)
from app.execution.paper_executor import PaperExecutor
from app.runner.market_snapshot import MarketSnapshot, claim_candle
from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import (
    ORGANIC_PROVENANCE,
    PAPER_FORWARD_VALIDATION,
)
from shared_lib.persistence.migrations import migrate

# ── Fresh validation identity (§38/§39) ─────────────────────────────────────

VALIDATION_BOT = "bot_phase12_validation"
VALIDATION_USER = "user_phase12_validation"
BROKER_ACCOUNT = "brk_phase12_validation"
POLICY_HASH = "phase12_policy_hash_0001"
SYMBOL = "BTCUSDT"
TIMEFRAME = "15m"
PROVENANCE = PAPER_FORWARD_VALIDATION

#: Bots that this smoke must never touch (§38).
FORBIDDEN_BOTS = ("bot_e5fe913972a9", "bot_a8117dc719fc")

TF_MS = 15 * 60 * 1000
QTY = 1.0
TP1_QTY = 0.5
TOLERANCE = 1e-9


class PaperOnlyClient:
    """Market data only. Every order API raises — this is the mainnet guard."""

    def __init__(self, price: float = 100.0) -> None:
        self.price = price
        self.live_order_calls: list[str] = []

    def get_prices(self, symbols):
        return {s: float(self.price) for s in symbols}

    def get_ticker(self, symbol):
        return {"symbol": symbol, "lastPrice": str(self.price)}

    def last_price(self, symbol):
        return float(self.price)

    def klines(self, **kw):
        return []

    def _forbidden(self, name):
        self.live_order_calls.append(name)
        raise AssertionError(f"Phase 12 must not reach the broker: {name}")

    def place_order(self, *a, **k):
        self._forbidden("place_order")

    def close_position_market(self, *a, **k):
        self._forbidden("close_position_market")

    def get_position_info(self, *a, **k):
        self._forbidden("get_position_info")

    def get_position_amt(self, *a, **k):
        self._forbidden("get_position_amt")

    def update_protection(self, *a, **k):
        self._forbidden("update_protection")


def candles(n: int = 40):
    return [
        [i * TF_MS, 100.0, 101.0, 99.0, 100.0, 10.0, i * TF_MS + TF_MS - 1]
        for i in range(1, n + 1)
    ]


@pytest.fixture
def client():
    return PaperOnlyClient()


@pytest.fixture
def paper(client):
    return PaperExecutor(client=client)


@pytest.fixture
def db():
    """An isolated database. The smoke never touches runtime evidence."""
    database = DB(":memory:")
    migrate(database)
    return database


@pytest.fixture
def runtime(db):
    """A fresh runtime session, bot run and cycle for the validation bot."""
    session_id = open_runtime_session(
        db, database_role="development", database_path=":memory:",
        process_execution_mode="paper", environment_name="phase12-smoke",
    )
    open_bot_run(
        db, run_id="run_phase12", bot_instance_id=VALIDATION_BOT,
        runtime_session_id=session_id, user_id=VALIDATION_USER,
        policy_hash=POLICY_HASH, provenance=PROVENANCE,
        execution_mode="paper", broker_environment="demo",
    )
    record_trading_cycle(db, {
        "cycle_id": "cyc_phase12", "run_id": "run_phase12",
        "bot_instance_id": VALIDATION_BOT, "policy_hash": POLICY_HASH,
        "provenance": PROVENANCE,
    })
    record_bot_lifecycle_event(
        db, bot_instance_id=VALIDATION_BOT, event_type="BOT_CREATED",
        actor="test:phase12_smoke", reason="Phase 12 paper runtime validation",
        previous_state=None, new_state="active", correlation_id="run_phase12",
    )
    return {"session_id": session_id, "run_id": "run_phase12", "cycle_id": "cyc_phase12"}


def evidence_kwargs(**extra):
    return dict(
        bot_instance_id=VALIDATION_BOT, run_id="run_phase12", cycle_id="cyc_phase12",
        provenance=PROVENANCE, **extra,
    )


# ══════════════════════════════════════════════════════════════════════════
# §38/§39/§40 — identity, isolation and the mainnet prohibition
# ══════════════════════════════════════════════════════════════════════════


def test_the_smoke_uses_a_fresh_bot_and_never_the_forensic_ones():
    assert VALIDATION_BOT not in FORBIDDEN_BOTS
    for forbidden in FORBIDDEN_BOTS:
        assert forbidden != VALIDATION_BOT


def test_no_forbidden_bot_id_appears_anywhere_in_this_module():
    """The unresolved September bots must not be reactivated or reused."""
    import inspect
    import sys

    source = inspect.getsource(sys.modules[__name__])
    body = source.split("FORBIDDEN_BOTS = ", 1)[1].split("\n", 1)[1]
    for forbidden in FORBIDDEN_BOTS:
        assert body.count(forbidden) == 0, f"{forbidden} is referenced outside the guard list"


def test_validation_provenance_can_never_count_toward_readiness():
    assert PROVENANCE == PAPER_FORWARD_VALIDATION
    assert PROVENANCE not in ORGANIC_PROVENANCE


def test_the_bot_contract_is_recorded_with_its_policy_hash(db, runtime):
    with db.connect() as conn:
        run = dict(conn.execute(
            "SELECT * FROM bot_runs WHERE run_id=?", ("run_phase12",)
        ).fetchone())

    assert run["bot_instance_id"] == VALIDATION_BOT
    assert run["policy_hash"] == POLICY_HASH
    assert run["execution_mode"] == "paper"
    assert run["broker_environment"] == "demo"
    assert run["provenance"] == PROVENANCE
    assert run["runtime_session_id"] == runtime["session_id"]


def test_execution_mode_is_paper_and_no_broker_order_api_is_reachable(paper, client):
    paper.open_position(symbol=SYMBOL, side="LONG", notional_usdt=0.0,
                        quantity=QTY, fallback_price=100.0)
    paper.partial_close(symbol=SYMBOL, position_side="LONG", quantity=TP1_QTY)
    paper.close_position(symbol=SYMBOL, position_side="LONG", quantity=TP1_QTY)

    assert client.live_order_calls == []


# ══════════════════════════════════════════════════════════════════════════
# §41 — new-candle proof
# ══════════════════════════════════════════════════════════════════════════


def test_one_closed_candle_produces_exactly_one_decision(db, runtime):
    rows = candles()
    snapshot = MarketSnapshot.build(symbol=SYMBOL, timeframe=TIMEFRAME,
                                    candles=rows, source="phase12")
    record_market_snapshot(db, snapshot, bot_instance_id=VALIDATION_BOT, provenance=PROVENANCE)

    claimed = claim_candle(db, bot_instance_id=VALIDATION_BOT, symbol=SYMBOL,
                           timeframe=TIMEFRAME, close_time=snapshot.latest_closed_candle_time)
    assert claimed is True

    with record_decision(db, symbol=SYMBOL, **evidence_kwargs()) as decision:
        decision.set_snapshot(snapshot).hold(QualityReason.NO_OPPORTUNITY)

    with db.connect() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM trading_decisions WHERE closed_candle_close_time=?",
            (snapshot.latest_closed_candle_time,),
        ).fetchone()[0]
    assert count == 1


def test_repeated_heartbeats_on_the_same_candle_yield_no_new_candle(db, runtime):
    snapshot = MarketSnapshot.build(symbol=SYMBOL, timeframe=TIMEFRAME,
                                    candles=candles(), source="phase12")
    close_time = snapshot.latest_closed_candle_time

    evaluations = 0
    for _ in range(6):  # one minute of 10-second heartbeats
        if claim_candle(db, bot_instance_id=VALIDATION_BOT, symbol=SYMBOL,
                        timeframe=TIMEFRAME, close_time=close_time):
            evaluations += 1
            with record_decision(db, symbol=SYMBOL, **evidence_kwargs()) as decision:
                decision.set_snapshot(snapshot).hold(QualityReason.NO_OPPORTUNITY)
        else:
            record_no_new_candle(db, bot_instance_id=VALIDATION_BOT, symbol=SYMBOL,
                                 timeframe=TIMEFRAME, run_id="run_phase12",
                                 cycle_id="cyc_phase12", provenance=PROVENANCE)

    assert evaluations == 1, "the strategy ran once for one closed candle"
    with db.connect() as conn:
        reasons = dict(conn.execute(
            "SELECT primary_reason, COUNT(*) FROM trading_decisions GROUP BY primary_reason"
        ).fetchall())
    assert reasons[CycleReason.NO_NEW_CANDLE] == 5
    assert reasons[QualityReason.NO_OPPORTUNITY] == 1


# ══════════════════════════════════════════════════════════════════════════
# §42/§43 — a controlled approved opportunity reaching PaperExecutor
# ══════════════════════════════════════════════════════════════════════════


def approved_opportunity(snapshot):
    """A deterministic opportunity injected BELOW strategy-quality generation.

    Production thresholds are untouched; the real TradingDecisionEngine still
    performs the one entry-quality comparison against them.
    """
    return build_opportunity(
        symbol=SYMBOL, timeframe=TIMEFRAME,
        market_snapshot_id=snapshot.market_snapshot_id, side="BUY",
        raw_confidence=0.92, consensus=0.92, buy_score=0.92, sell_score=0.05,
        votes=[("supertrend", "BUY", 0.9), ("donchian", "BUY", 0.85)],
        regime="TREND", regime_confidence=0.8,
        closed_candle_time=snapshot.latest_closed_candle_time,
    )


def test_the_full_open_path_produces_correlated_evidence(db, runtime, paper, client):
    snapshot = MarketSnapshot.build(symbol=SYMBOL, timeframe=TIMEFRAME,
                                    candles=candles(), source="phase12")
    record_market_snapshot(db, snapshot, bot_instance_id=VALIDATION_BOT, provenance=PROVENANCE)
    opportunity = approved_opportunity(snapshot)

    # The real engine, against a real (unlowered) threshold.
    engine = TradingDecisionEngine()
    quality = engine.evaluate(opportunity, base_threshold=0.55)
    assert quality.approved is True

    with record_decision(db, symbol=SYMBOL, **evidence_kwargs()) as decision:
        decision.set_snapshot(snapshot).set_opportunity(opportunity).set_entry_quality(quality)
        decision.set_risk(approved=True, reason="RISK_APPROVED", quantity=QTY,
                          approved_notional=100.0, leverage=1.0, resolved_rr=1.8)
        decision.set_execution_feasibility(approved=True, reason="EXECUTION_APPROVED")
        decision.set_entry_protection(approved=True, reason="OK")

        attempt_id = open_execution_attempt(
            db, decision_id=decision.decision_id, bot_instance_id=VALIDATION_BOT,
            symbol=SYMBOL, requested_action="BUY", execution_mode="paper",
            broker_environment="demo", provenance=PROVENANCE,
            run_id="run_phase12", cycle_id="cyc_phase12", requested_qty=QTY,
        )
        result = paper.open_position(symbol=SYMBOL, side="LONG", notional_usdt=0.0,
                                     quantity=QTY, fallback_price=100.0)
        assert result.success is True
        position_id = result.details["position_id"]

        record_position_opened(
            db, position_id=position_id, bot_instance_id=VALIDATION_BOT, symbol=SYMBOL,
            side="LONG", original_qty=result.filled_qty, entry_price=result.avg_price,
            provenance=PROVENANCE, run_id="run_phase12",
            decision_id=decision.decision_id, execution_attempt_id=attempt_id,
            execution_mode="paper", broker_environment="demo",
        )
        complete_execution_attempt(db, attempt_id, result="SUCCESS",
                                   broker_order_id=result.order_id, position_id=position_id)

        decision.execution_attempt_id = attempt_id
        decision.order_id = result.order_id
        decision.position_id = position_id
        decision.approve(QualityReason.APPROVED_FOR_EXECUTION)
        decision_id = decision.decision_id

    # Every artefact shares the same correlation ids.
    with db.connect() as conn:
        row = conn.execute(
            """SELECT d.decision_id, d.market_snapshot_id, a.execution_attempt_id,
                      p.position_id, e.event_type
               FROM trading_decisions d
               JOIN execution_attempts a ON a.decision_id = d.decision_id
               JOIN positions p ON p.position_id = a.position_id
               JOIN position_events e ON e.position_id = p.position_id
               WHERE d.decision_id = ?""",
            (decision_id,),
        ).fetchone()

    assert row is not None
    assert row["market_snapshot_id"] == snapshot.market_snapshot_id
    assert row["event_type"] == "OPENED"
    assert client.live_order_calls == []


# ══════════════════════════════════════════════════════════════════════════
# §44/§45/§46/§47 — TP1, break-even, restart and final close
# ══════════════════════════════════════════════════════════════════════════


@pytest.fixture
def open_position(db, runtime, paper):
    """A validation position that is open with 1.0 units."""
    result = paper.open_position(symbol=SYMBOL, side="LONG", notional_usdt=0.0,
                                 quantity=QTY, fallback_price=100.0)
    position_id = result.details["position_id"]
    record_position_opened(
        db, position_id=position_id, bot_instance_id=VALIDATION_BOT, symbol=SYMBOL,
        side="LONG", original_qty=QTY, entry_price=result.avg_price,
        provenance=PROVENANCE, run_id="run_phase12", execution_mode="paper",
    )
    return position_id


def test_tp1_reduces_the_remainder_and_records_a_lifecycle_event(db, paper, open_position):
    tp1 = paper.partial_close(symbol=SYMBOL, position_side="LONG", quantity=TP1_QTY)
    assert tp1.success is True

    update_position_quantities(db, open_position, remaining_qty=TP1_QTY, realized_qty=TP1_QTY,
                               realized_pnl=tp1.details["realized_pnl"], fees=tp1.fee)
    record_position_event(db, position_id=open_position, bot_instance_id=VALIDATION_BOT,
                          symbol=SYMBOL, event_type="TP1", quantity=TP1_QTY,
                          remaining_qty=TP1_QTY, price=tp1.avg_price, provenance=PROVENANCE)

    with db.connect() as conn:
        pos = dict(conn.execute(
            "SELECT * FROM positions WHERE position_id=?", (open_position,)
        ).fetchone())

    assert pos["original_qty"] == pytest.approx(QTY)
    assert pos["realized_qty"] == pytest.approx(TP1_QTY)
    assert pos["remaining_qty"] == pytest.approx(TP1_QTY)
    # original - partial = remaining
    assert pos["original_qty"] - pos["realized_qty"] == pytest.approx(pos["remaining_qty"])


def test_break_even_and_trailing_operate_on_the_post_tp1_remainder(db, paper, open_position):
    paper.partial_close(symbol=SYMBOL, position_side="LONG", quantity=TP1_QTY)
    update_position_quantities(db, open_position, remaining_qty=TP1_QTY, realized_qty=TP1_QTY)

    for event_type, stop in (("BREAK_EVEN_ACTIVATED", 100.0), ("TRAILING_ACTIVATED", 102.0)):
        record_position_event(db, position_id=open_position, bot_instance_id=VALIDATION_BOT,
                              symbol=SYMBOL, event_type=event_type,
                              remaining_qty=TP1_QTY, stop_price=stop, provenance=PROVENANCE)

    with db.connect() as conn:
        events = [dict(r) for r in conn.execute(
            "SELECT event_type, remaining_qty FROM position_events "
            "WHERE position_id=? AND event_type IN ('BREAK_EVEN_ACTIVATED','TRAILING_ACTIVATED')",
            (open_position,),
        )]
    assert len(events) == 2
    for event in events:
        assert event["remaining_qty"] == pytest.approx(TP1_QTY), (
            "protection must track the remainder, not the original size"
        )


def test_restart_restores_the_partial_remainder_not_the_original(db, client, paper, open_position):
    paper.partial_close(symbol=SYMBOL, position_side="LONG", quantity=TP1_QTY)
    update_position_quantities(db, open_position, remaining_qty=TP1_QTY, realized_qty=TP1_QTY)

    # Simulate a process restart: a brand-new executor rehydrated from the DB.
    with db.connect() as conn:
        persisted = dict(conn.execute(
            "SELECT * FROM positions WHERE position_id=?", (open_position,)
        ).fetchone())

    restarted = PaperExecutor(client=client)
    restarted.seed_position(
        SYMBOL, persisted["side"], persisted["remaining_qty"], persisted["entry_price"],
        position_id=persisted["position_id"], original_quantity=persisted["original_qty"],
        realized_quantity=persisted["realized_qty"],
    )

    assert restarted.remaining_quantity(SYMBOL) == pytest.approx(TP1_QTY)
    assert restarted.remaining_quantity(SYMBOL) != pytest.approx(QTY)
    assert restarted.get_position(SYMBOL)["original_qty"] == pytest.approx(QTY)


def test_final_close_closes_only_the_remainder_and_reaches_flat(db, paper, open_position, client):
    paper.partial_close(symbol=SYMBOL, position_side="LONG", quantity=TP1_QTY)
    update_position_quantities(db, open_position, remaining_qty=TP1_QTY, realized_qty=TP1_QTY)

    remaining = paper.remaining_quantity(SYMBOL)
    close = paper.close_position(symbol=SYMBOL, position_side="LONG", quantity=remaining)

    assert close.success is True
    assert close.filled_qty == pytest.approx(TP1_QTY), "must not close the original 1.0"

    update_position_quantities(db, open_position, remaining_qty=0.0, realized_qty=QTY,
                               status="CLOSED", close_reason="FINAL_CLOSE")
    record_position_event(db, position_id=open_position, bot_instance_id=VALIDATION_BOT,
                          symbol=SYMBOL, event_type="FINAL_CLOSE", quantity=TP1_QTY,
                          remaining_qty=0.0, price=close.avg_price, fee=close.fee,
                          provenance=PROVENANCE)

    with db.connect() as conn:
        pos = dict(conn.execute(
            "SELECT * FROM positions WHERE position_id=?", (open_position,)
        ).fetchone())

    assert pos["status"] == "CLOSED"
    assert pos["remaining_qty"] == pytest.approx(0.0)
    assert pos["closed_at"]
    assert client.live_order_calls == []


def test_the_accounting_invariant_holds_across_the_whole_lifecycle(db, paper, open_position):
    opened = QTY
    tp1 = paper.partial_close(symbol=SYMBOL, position_side="LONG", quantity=TP1_QTY)
    close = paper.close_position(symbol=SYMBOL, position_side="LONG",
                                 quantity=paper.remaining_quantity(SYMBOL))

    residual = opened - tp1.filled_qty - close.filled_qty
    assert abs(residual) <= TOLERANCE, f"unaccounted quantity: {residual}"


def test_restart_after_full_close_stays_flat(db, client, paper, open_position):
    paper.partial_close(symbol=SYMBOL, position_side="LONG", quantity=TP1_QTY)
    paper.close_position(symbol=SYMBOL, position_side="LONG", quantity=TP1_QTY)
    update_position_quantities(db, open_position, remaining_qty=0.0, realized_qty=QTY,
                               status="CLOSED", close_reason="FINAL_CLOSE")

    restarted = PaperExecutor(client=client)
    with db.connect() as conn:
        still_open = conn.execute(
            "SELECT COUNT(*) FROM positions WHERE bot_instance_id=? AND status='OPEN'",
            (VALIDATION_BOT,),
        ).fetchone()[0]

    assert still_open == 0
    assert restarted.get_position(SYMBOL) is None
    assert restarted.remaining_quantity(SYMBOL) is None  # unknown, not a phantom zero


# ══════════════════════════════════════════════════════════════════════════
# §51 — reconciliation across every authority
# ══════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("stage", ["after_open", "after_tp1", "after_close"])
def test_all_quantity_authorities_agree_at_every_stage(db, paper, open_position, stage):
    expected = QTY

    if stage in ("after_tp1", "after_close"):
        paper.partial_close(symbol=SYMBOL, position_side="LONG", quantity=TP1_QTY)
        update_position_quantities(db, open_position, remaining_qty=TP1_QTY, realized_qty=TP1_QTY)
        expected = TP1_QTY

    if stage == "after_close":
        paper.close_position(symbol=SYMBOL, position_side="LONG", quantity=TP1_QTY)
        update_position_quantities(db, open_position, remaining_qty=0.0, realized_qty=QTY,
                                   status="CLOSED", close_reason="FINAL_CLOSE")
        expected = 0.0

    with db.connect() as conn:
        persisted = dict(conn.execute(
            "SELECT * FROM positions WHERE position_id=?", (open_position,)
        ).fetchone())

    executor_qty = paper.remaining_quantity(SYMBOL)
    if stage == "after_close":
        assert executor_qty is None  # the simulated position is gone
    else:
        assert executor_qty == pytest.approx(expected)

    assert persisted["remaining_qty"] == pytest.approx(expected)
    assert (
        persisted["original_qty"] - persisted["realized_qty"] - persisted["remaining_qty"]
        == pytest.approx(0.0, abs=TOLERANCE)
    )


# ══════════════════════════════════════════════════════════════════════════
# §48 — daily close
# ══════════════════════════════════════════════════════════════════════════


def test_daily_close_produces_exactly_one_close_and_one_mark(db, paper, open_position):
    window = "2026-09-08:23:30"

    def daily_close_tick() -> bool:
        """One heartbeat's worth of daily-close logic, marker included."""
        with db.connect() as conn:
            already = conn.execute(
                """SELECT 1 FROM bot_daily_close_marks
                   WHERE bot_instance_id=? AND symbol=? AND close_window=?""",
                (VALIDATION_BOT, SYMBOL, window),
            ).fetchone()
        if already:
            return False
        remaining = paper.remaining_quantity(SYMBOL)
        if not remaining:
            return False
        paper.close_position(symbol=SYMBOL, position_side="LONG", quantity=remaining)
        update_position_quantities(db, open_position, remaining_qty=0.0, realized_qty=QTY,
                                   status="CLOSED", close_reason="DAILY_CLOSE")
        record_position_event(db, position_id=open_position, bot_instance_id=VALIDATION_BOT,
                              symbol=SYMBOL, event_type="DAILY_CLOSE", quantity=remaining,
                              remaining_qty=0.0, provenance=PROVENANCE)
        with db.connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO bot_daily_close_marks
                   (bot_instance_id,symbol,close_window,position_id,closed_at,reason)
                   VALUES (?,?,?,?,?,?)""",
                (VALIDATION_BOT, SYMBOL, window, open_position, "now", "DAILY_CLOSE"),
            )
        return True

    closes = sum(daily_close_tick() for _ in range(10))  # ten heartbeats in the window

    assert closes == 1, "repeated heartbeats must not re-close"
    with db.connect() as conn:
        events = conn.execute(
            "SELECT COUNT(*) FROM position_events WHERE event_type='DAILY_CLOSE'"
        ).fetchone()[0]
        marks = conn.execute("SELECT COUNT(*) FROM bot_daily_close_marks").fetchone()[0]
    assert events == 1
    assert marks == 1


def test_a_restart_inside_the_same_window_does_not_repeat_the_close(db, open_position):
    window = "2026-09-08:23:30"
    with db.connect() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO bot_daily_close_marks
               (bot_instance_id,symbol,close_window,position_id,closed_at,reason)
               VALUES (?,?,?,?,?,?)""",
            (VALIDATION_BOT, SYMBOL, window, open_position, "now", "DAILY_CLOSE"),
        )

    # A fresh process re-reads the durable marker rather than an in-memory flag.
    with db.connect() as conn:
        already = conn.execute(
            """SELECT 1 FROM bot_daily_close_marks
               WHERE bot_instance_id=? AND symbol=? AND close_window=?""",
            (VALIDATION_BOT, SYMBOL, window),
        ).fetchone()
    assert already is not None


def test_the_next_days_window_is_a_different_window(db, open_position):
    with db.connect() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO bot_daily_close_marks
               (bot_instance_id,symbol,close_window,position_id,closed_at,reason)
               VALUES (?,?,?,?,?,?)""",
            (VALIDATION_BOT, SYMBOL, "2026-09-08:23:30", open_position, "now", "DAILY_CLOSE"),
        )
        tomorrow = conn.execute(
            """SELECT 1 FROM bot_daily_close_marks
               WHERE bot_instance_id=? AND symbol=? AND close_window=?""",
            (VALIDATION_BOT, SYMBOL, "2026-09-09:23:30"),
        ).fetchone()
    assert tomorrow is None


# ══════════════════════════════════════════════════════════════════════════
# §49 — kill switch
# ══════════════════════════════════════════════════════════════════════════


def test_kill_switch_blocks_new_entries(db, runtime):
    """With the switch active, an evaluation terminates before execution."""
    from app.decision.reasons import LifecycleReason

    with record_decision(db, symbol=SYMBOL, **evidence_kwargs()) as decision:
        decision.reject(LifecycleReason.KILL_SWITCH_ACTIVE)

    with db.connect() as conn:
        row = dict(conn.execute("SELECT * FROM trading_decisions").fetchone())
        attempts = conn.execute("SELECT COUNT(*) FROM execution_attempts").fetchone()[0]

    assert row["primary_reason"] == "KILL_SWITCH_ACTIVE"
    assert row["final_action"] == "REJECTED"
    assert attempts == 0, "a blocked entry must not produce an execution attempt"


def test_kill_switch_flatten_persists_close_evidence(db, paper, open_position):
    from app.decision.reasons import LifecycleReason

    remaining = paper.remaining_quantity(SYMBOL)
    close = paper.close_position(symbol=SYMBOL, position_side="LONG", quantity=remaining)
    assert close.success is True

    update_position_quantities(db, open_position, remaining_qty=0.0, realized_qty=QTY,
                               status="CLOSED", close_reason=LifecycleReason.KILL_SWITCH_ACTIVE)
    record_position_event(db, position_id=open_position, bot_instance_id=VALIDATION_BOT,
                          symbol=SYMBOL, event_type="KILL_SWITCH_CLOSE", quantity=remaining,
                          remaining_qty=0.0, reason=LifecycleReason.KILL_SWITCH_ACTIVE,
                          provenance=PROVENANCE)

    with db.connect() as conn:
        pos = dict(conn.execute(
            "SELECT * FROM positions WHERE position_id=?", (open_position,)
        ).fetchone())
        event = dict(conn.execute(
            "SELECT * FROM position_events WHERE event_type='KILL_SWITCH_CLOSE'"
        ).fetchone())

    assert pos["status"] == "CLOSED"
    assert pos["close_reason"] == "KILL_SWITCH_ACTIVE"
    assert event["reason"] == "KILL_SWITCH_ACTIVE"


# ══════════════════════════════════════════════════════════════════════════
# §50 — duplicate-entry protection
# ══════════════════════════════════════════════════════════════════════════


def test_the_same_candle_yields_at_most_one_execution_attempt(db, runtime):
    snapshot = MarketSnapshot.build(symbol=SYMBOL, timeframe=TIMEFRAME,
                                    candles=candles(), source="phase12")
    close_time = snapshot.latest_closed_candle_time

    attempts = 0
    for _ in range(5):  # repeated ticks and a restart in between
        if not claim_candle(db, bot_instance_id=VALIDATION_BOT, symbol=SYMBOL,
                            timeframe=TIMEFRAME, close_time=close_time):
            continue
        attempts += 1
        open_execution_attempt(db, decision_id="dec-dup", bot_instance_id=VALIDATION_BOT,
                               symbol=SYMBOL, requested_action="BUY", provenance=PROVENANCE)

    with db.connect() as conn:
        stored = conn.execute("SELECT COUNT(*) FROM execution_attempts").fetchone()[0]

    assert attempts == 1
    assert stored == 1


def test_a_duplicate_open_is_refused_by_the_paper_executor(paper, open_position):
    """EntryProtection remains the final barrier; the simulator agrees."""
    result = paper.partial_close(symbol=SYMBOL, position_side="LONG", quantity=QTY)
    assert result.success is False, "cannot close more than the position holds"

    assert paper.remaining_quantity(SYMBOL) == pytest.approx(QTY)


# ══════════════════════════════════════════════════════════════════════════
# §53/§54 — integrity and diagnostics over the smoke run
# ══════════════════════════════════════════════════════════════════════════


def test_the_smoke_run_leaves_no_integrity_violations(db, runtime, paper):
    snapshot = MarketSnapshot.build(symbol=SYMBOL, timeframe=TIMEFRAME,
                                    candles=candles(), source="phase12")
    record_market_snapshot(db, snapshot, bot_instance_id=VALIDATION_BOT, provenance=PROVENANCE)

    with record_decision(db, symbol=SYMBOL, **evidence_kwargs()) as decision:
        decision.set_snapshot(snapshot)
        attempt_id = open_execution_attempt(
            db, decision_id=decision.decision_id, bot_instance_id=VALIDATION_BOT,
            symbol=SYMBOL, requested_action="BUY", provenance=PROVENANCE,
        )
        result = paper.open_position(symbol=SYMBOL, side="LONG", notional_usdt=0.0,
                                     quantity=QTY, fallback_price=100.0)
        position_id = result.details["position_id"]
        record_position_opened(db, position_id=position_id, bot_instance_id=VALIDATION_BOT,
                               symbol=SYMBOL, side="LONG", original_qty=QTY,
                               entry_price=result.avg_price, provenance=PROVENANCE)
        complete_execution_attempt(db, attempt_id, result="SUCCESS",
                                   broker_order_id=result.order_id, position_id=position_id)
        decision.execution_attempt_id = attempt_id
        decision.position_id = position_id
        decision.approve(QualityReason.APPROVED_FOR_EXECUTION)

    paper.close_position(symbol=SYMBOL, position_side="LONG", quantity=QTY)
    update_position_quantities(db, position_id, remaining_qty=0.0, realized_qty=QTY,
                               status="CLOSED", close_reason="FINAL_CLOSE")
    record_position_event(db, position_id=position_id, bot_instance_id=VALIDATION_BOT,
                          symbol=SYMBOL, event_type="FINAL_CLOSE", quantity=QTY,
                          remaining_qty=0.0, provenance=PROVENANCE)

    assert run_integrity_checks(db) == {}


def test_diagnostics_can_explain_why_the_bot_did_not_trade(db, runtime):
    for reason in (CycleReason.NO_NEW_CANDLE,) * 12 + (
        QualityReason.REGIME_BLOCKED, QualityReason.REGIME_BLOCKED,
        QualityReason.ENTRY_CONFIDENCE_BELOW_THRESHOLD,
        QualityReason.ENTRY_CONFIDENCE_BELOW_THRESHOLD,
    ):
        with record_decision(db, symbol=SYMBOL, **evidence_kwargs()) as decision:
            decision.hold(reason)

    with db.connect() as conn:
        counts = dict(conn.execute(
            "SELECT primary_reason, COUNT(*) FROM trading_decisions "
            "WHERE bot_instance_id=? GROUP BY primary_reason", (VALIDATION_BOT,)
        ).fetchall())
        attempts = conn.execute("SELECT COUNT(*) FROM execution_attempts").fetchone()[0]

    assert sum(counts.values()) == 16
    assert counts["NO_NEW_CANDLE"] == 12
    assert counts["REGIME_BLOCKED"] == 2
    assert counts["ENTRY_CONFIDENCE_BELOW_THRESHOLD"] == 2
    assert attempts == 0


def test_diagnostics_can_explain_why_the_bot_did_trade(db, runtime, paper):
    """§54 — one decision id yields the whole causal chain."""
    snapshot = MarketSnapshot.build(symbol=SYMBOL, timeframe=TIMEFRAME,
                                    candles=candles(), source="phase12")
    record_market_snapshot(db, snapshot, bot_instance_id=VALIDATION_BOT, provenance=PROVENANCE)
    opportunity = approved_opportunity(snapshot)
    quality = TradingDecisionEngine().evaluate(opportunity, base_threshold=0.55)

    with record_decision(db, symbol=SYMBOL, **evidence_kwargs()) as decision:
        decision.set_snapshot(snapshot).set_opportunity(opportunity).set_entry_quality(quality)
        decision.set_risk(approved=True, reason="RISK_APPROVED", quantity=QTY, resolved_rr=1.8)
        decision.set_execution_feasibility(approved=True, reason="EXECUTION_APPROVED")
        decision.approve(QualityReason.APPROVED_FOR_EXECUTION)
        decision_id = decision.decision_id

    with db.connect() as conn:
        row = dict(conn.execute(
            "SELECT * FROM trading_decisions WHERE decision_id=?", (decision_id,)
        ).fetchone())

    # Everything §54 requires, from one row plus its snapshot join.
    assert row["market_snapshot_id"] == snapshot.market_snapshot_id
    assert row["opportunity_id"] == opportunity.opportunity_id
    assert row["quality_result"] == "PASS"
    assert row["risk_result"] == "PASS"
    assert row["execution_feasibility_result"] == "PASS"
    assert row["resolved_rr"] == pytest.approx(1.8)
    assert row["regime"] == "TREND"
    assert row["raw_confidence"] == pytest.approx(0.92)
    assert row["effective_entry_threshold"] == pytest.approx(0.55)
    assert row["final_action"] == "APPROVED"
    assert row["complete"] == 1


def test_profitability_is_irrelevant_to_this_smoke(db, paper, open_position, client):
    """§52 — the smoke may lose money. Only the plumbing is under test."""
    client.price = 80.0  # a 20% adverse move
    close = paper.close_position(symbol=SYMBOL, position_side="LONG",
                                 quantity=paper.remaining_quantity(SYMBOL))
    update_position_quantities(db, open_position, remaining_qty=0.0, realized_qty=QTY,
                               status="CLOSED", close_reason="FINAL_CLOSE")
    record_position_event(db, position_id=open_position, bot_instance_id=VALIDATION_BOT,
                          symbol=SYMBOL, event_type="FINAL_CLOSE", quantity=QTY,
                          remaining_qty=0.0, provenance=PROVENANCE)

    assert close.success is True
    assert run_integrity_checks(db) == {}, "a losing trade is still valid evidence"
