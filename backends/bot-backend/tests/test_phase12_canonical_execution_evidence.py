"""Phase 12 — the canonical execution lineage, and the defects that broke it.

Every test here corresponds to something that was actually wrong, found by
driving a real ``PaperRunner`` through a full paper lifecycle rather than by
calling the evidence writers directly:

* the canonical chain stopped at ``trading_decisions`` — ``execution_attempts``,
  ``positions`` and ``position_events`` had no production caller at all, and
  zero rows in the runtime database after 4,478 decisions;
* ``ensure_protection`` read broker position truth in paper mode, so an open
  paper position looked flat on the next cycle and was marked FLAT;
* the same function called ``cancel_all_orders`` — a broker order endpoint —
  from a paper bot;
* the TP1 duplicate guard treated ``TP1_TAKEN`` as "already done", but
  ``update_price`` sets exactly that phase immediately before returning
  ``HIT_TP1``, so no TP1 partial close could ever execute;
* the PositionManager "restore" branch was the ``else`` of the trailing-stop
  condition, so it re-armed live positions on nearly every cycle, erasing TP1,
  break-even and trailing state.
"""
from __future__ import annotations

import inspect
from unittest.mock import MagicMock

import pytest

from app.evidence.fill_bridge import (
    execution_attempt,
    project_fill,
    sync_lifecycle_events,
)
from app.evidence.runner_bridge import run_provenance
from app.evidence.writers import open_bot_run, open_runtime_session
from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import (
    ORGANIC_PROVENANCE,
    PAPER_FORWARD,
    PAPER_FORWARD_VALIDATION,
)
from shared_lib.persistence.migrations import migrate

BOT = "bot_phase12_evidence"
SYMBOL = "BTCUSDT"
RUN_ID = "run_phase12_evidence"


@pytest.fixture
def db():
    database = DB(":memory:")
    migrate(database)
    return database


class FakeContext:
    bot_instance_id = BOT
    user_id = "user_phase12"
    broker_account_id = "brk_phase12"
    broker_environment = "demo"
    market_type = "CRYPTO"
    effective_policy_hash = "policy_phase12"


class FakeRunner:
    """The minimum surface the evidence bridge reads off a runner."""

    def __init__(self, db, *, run_id: str = RUN_ID) -> None:
        self.db = db
        self.run_id = run_id
        self.cycle_id = "cyc_phase12"
        self.context = FakeContext()
        self.position_manager = None
        self._active_decision_ids = {SYMBOL: "dec_phase12"}

    def _effective_execution_mode(self) -> str:
        return "paper"


@pytest.fixture
def runner(db):
    session = open_runtime_session(
        db, database_role="development", database_path=":memory:",
        process_execution_mode="paper", environment_name="phase12-tests",
    )
    open_bot_run(
        db, run_id=RUN_ID, bot_instance_id=BOT, runtime_session_id=session,
        user_id="user_phase12", policy_hash="policy_phase12",
        provenance=PAPER_FORWARD_VALIDATION, execution_mode="paper",
        broker_environment="demo",
    )
    return FakeRunner(db)


def fill(**overrides):
    base = {
        "symbol": SYMBOL, "side": "LONG", "action": "OPEN",
        "qty": 100.0, "price": 10.0, "position_id": "pos_phase12",
    }
    base.update(overrides)
    return base


def positions(db):
    with db.connect() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM positions")]


def events(db):
    with db.connect() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM position_events ORDER BY occurred_at, rowid"
        )]


# ══════════════════════════════════════════════════════════════════════════
# The chain the runtime never wrote
# ══════════════════════════════════════════════════════════════════════════


def test_an_open_fill_creates_the_canonical_position(db, runner):
    project_fill(runner, db, fill())

    (row,) = positions(db)
    assert row["side"] == "LONG"
    assert row["original_qty"] == 100.0
    assert row["remaining_qty"] == 100.0
    assert row["realized_qty"] == 0.0
    assert row["status"] == "OPEN"
    assert row["decision_id"] == "dec_phase12"
    assert [e["event_type"] for e in events(db)] == ["OPENED"]


def test_a_tp1_partial_leaves_the_remainder_open(db, runner):
    project_fill(runner, db, fill())
    project_fill(runner, db, fill(
        action="PARTIAL_CLOSE", qty=40.0, price=12.0,
        fill_type="TP1", realized_pnl=80.0, fee=1.0,
    ))

    (row,) = positions(db)
    assert row["original_qty"] == 100.0
    assert row["remaining_qty"] == 60.0
    assert row["realized_qty"] == 40.0
    assert row["status"] == "OPEN"
    assert row["realized_pnl"] == 80.0

    types = [e["event_type"] for e in events(db)]
    assert types == ["OPENED", "TP1"]


def test_the_accounting_invariant_holds_to_flat(db, runner):
    project_fill(runner, db, fill())
    project_fill(runner, db, fill(action="PARTIAL_CLOSE", qty=40.0, fill_type="TP1"))
    project_fill(runner, db, fill(action="CLOSE", qty=60.0, price=13.0,
                                  realized_pnl=180.0, exit_reason="TRAILING_SL"))

    (row,) = positions(db)
    assert row["remaining_qty"] == pytest.approx(0.0, abs=1e-9)
    assert row["realized_qty"] == pytest.approx(100.0, abs=1e-9)
    assert row["status"] == "CLOSED"
    assert row["closed_at"] is not None
    assert row["original_qty"] - row["realized_qty"] == pytest.approx(0.0, abs=1e-9)
    assert [e["event_type"] for e in events(db)] == ["OPENED", "TP1", "FINAL_CLOSE"]


@pytest.mark.parametrize(
    "reason,expected",
    [
        ("DAILY_CLOSE", "DAILY_CLOSE"),
        ("KILL_SWITCH_ACTIVE", "KILL_SWITCH_CLOSE"),
        ("TRAILING_SL", "FINAL_CLOSE"),
    ],
)
def test_close_reasons_get_their_own_event_type(db, runner, reason, expected):
    project_fill(runner, db, fill())
    project_fill(runner, db, fill(action="CLOSE", qty=100.0, exit_reason=reason))
    assert events(db)[-1]["event_type"] == expected


def test_a_close_for_an_unknown_position_is_reported_not_invented(db, runner):
    project_fill(runner, db, fill(action="CLOSE", qty=10.0, position_id="pos_ghost"))
    assert positions(db) == []


# ══════════════════════════════════════════════════════════════════════════
# Execution attempts — evidence that we tried, even when nothing filled
# ══════════════════════════════════════════════════════════════════════════


def attempts(db):
    with db.connect() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM execution_attempts")]


def test_an_attempt_is_recorded_before_the_executor_runs(db, runner):
    with execution_attempt(runner, SYMBOL, "BUY") as attempt:
        # Mid-flight: the row already exists and is IN_PROGRESS.
        (row,) = attempts(db)
        assert row["result"] == "IN_PROGRESS"
        assert row["decision_id"] == "dec_phase12"
        attempt.completed("PAPER_POSITION_OPENED", broker_order_id="paper_1")

    (row,) = attempts(db)
    assert row["result"] == "PAPER_POSITION_OPENED"
    assert row["broker_order_id"] == "paper_1"
    assert row["completed_at"] is not None


def test_an_executor_that_raises_still_leaves_evidence(db, runner):
    with pytest.raises(RuntimeError):
        with execution_attempt(runner, SYMBOL, "BUY"):
            raise RuntimeError("broker exploded")

    (row,) = attempts(db)
    assert row["result"] == "ERROR"
    assert row["error_class"] == "RuntimeError"
    assert "broker exploded" in row["error_detail"]


# ══════════════════════════════════════════════════════════════════════════
# Provenance: validation evidence must never look organic
# ══════════════════════════════════════════════════════════════════════════


def test_evidence_inherits_the_provenance_of_its_run(db, runner):
    project_fill(runner, db, fill())

    (row,) = positions(db)
    assert row["provenance"] == PAPER_FORWARD_VALIDATION
    assert row["provenance"] not in ORGANIC_PROVENANCE
    assert all(e["provenance"] == PAPER_FORWARD_VALIDATION for e in events(db))


def test_an_unknown_run_falls_back_rather_than_going_unlabelled(db):
    assert run_provenance(db, "run_that_does_not_exist", PAPER_FORWARD) == PAPER_FORWARD
    assert run_provenance(db, None, PAPER_FORWARD) == PAPER_FORWARD


# ══════════════════════════════════════════════════════════════════════════
# Lifecycle transitions that produce no fill
# ══════════════════════════════════════════════════════════════════════════


class FakePosition:
    def __init__(self, *, stop: float, break_even: bool, trailing: bool) -> None:
        self.position_id = "pos_phase12"
        self.current_qty = 60.0
        self.phase = "PositionPhase.RUNNER_TRAILING" if trailing else "PositionPhase.SEEKING_TP1"
        self.sl = MagicMock(current_stop=stop, is_break_even=break_even)


def with_position(runner, position):
    manager = MagicMock()
    manager.get_position.return_value = position
    runner.position_manager = manager
    return runner


def test_break_even_and_trailing_are_recorded_once(db, runner):
    project_fill(runner, db, fill())
    with_position(runner, FakePosition(stop=10.5, break_even=True, trailing=True))

    sync_lifecycle_events(runner, SYMBOL)
    sync_lifecycle_events(runner, SYMBOL)  # a second cycle must not duplicate

    types = [e["event_type"] for e in events(db)]
    assert types.count("BREAK_EVEN_ACTIVATED") == 1
    assert types.count("TRAILING_ACTIVATED") == 1


def test_a_moved_stop_is_recorded_as_its_own_event(db, runner):
    project_fill(runner, db, fill())
    with_position(runner, FakePosition(stop=10.5, break_even=True, trailing=True))
    sync_lifecycle_events(runner, SYMBOL)

    with_position(runner, FakePosition(stop=11.25, break_even=True, trailing=True))
    sync_lifecycle_events(runner, SYMBOL)

    stop_updates = [e for e in events(db) if e["event_type"] == "STOP_UPDATED"]
    assert len(stop_updates) == 1
    assert stop_updates[0]["stop_price"] == 11.25


def test_transitions_are_found_after_a_restart_not_only_within_one_process(db, runner):
    """The check is against the database, so a fresh process still sees them."""
    project_fill(runner, db, fill())

    # A brand-new runner object: no in-memory history at all.
    fresh = with_position(FakeRunner(db), FakePosition(stop=10.5, break_even=True, trailing=False))
    sync_lifecycle_events(fresh, SYMBOL)

    assert "BREAK_EVEN_ACTIVATED" in [e["event_type"] for e in events(db)]


# ══════════════════════════════════════════════════════════════════════════
# The four runtime defects, pinned so they cannot come back
# ══════════════════════════════════════════════════════════════════════════


def test_tp1_taken_is_not_treated_as_a_completed_tp1():
    """PositionManager sets TP1_TAKEN *before* returning HIT_TP1.

    Including it in the duplicate guard meant every TP1 arrived already
    "complete", was skipped with requested_tp1_qty=0.0, and the partial close
    could never fire.
    """
    from app.execution import executor as executor_module
    from app.execution.position_manager import PositionPhase

    source = _function_source(executor_module, "execute_tp1_partial_close")
    guard = source[source.index("safe_phases = {"):source.index("if pos.phase in safe_phases")]

    assert "TP1_TAKEN" not in guard.replace("# ", "")
    for phase in ("TP1_EXECUTING", "TP1_FILLED", "RUNNER_TRAILING", "EXITING"):
        assert phase in guard, f"{phase} must still guard against a duplicate TP1"

    assert PositionPhase.TP1_TAKEN.value == "TP1_TAKEN"


def test_update_price_sets_tp1_taken_before_signalling_hit_tp1():
    """The premise of the guard fix, asserted directly against the source."""
    from app.execution import position_manager as pm_module

    source = _function_source(pm_module, "update_price")
    marker = source.index("PositionPhase.TP1_TAKEN")
    signal = source.index('return "HIT_TP1"')
    assert marker < signal, "TP1_TAKEN must be set before HIT_TP1 is returned"


def test_the_position_manager_restore_is_not_the_else_of_a_trailing_check():
    """A live managed position must not be re-armed when trailing is not due.

    As the ``else`` of the trailing-stop condition, the restore ran on nearly
    every cycle and reset phase, tp1_hit and the stop.
    """
    from app.runner import runner as runner_module

    source = _function_source(runner_module, "_step_symbol_orchestrated")
    anchor = source.index("The PositionManager has no live position")
    # The 400 characters immediately before the restore body are its guard.
    guard = source[max(0, anchor - 400):anchor]

    assert "elif" in guard, "the restore must be conditional, not a bare else"
    assert "get_position(symbol)" in guard
    assert "PositionPhase.FLAT" in guard


def test_paper_mode_never_places_broker_protection():
    from app.execution import executor as executor_module

    source = _function_source(executor_module, "ensure_protection")
    start = source.index('if effective_mode == "paper"')
    # The branch runs to its second return; everything after is the broker path.
    end = source.index('"paper": True', start)
    branch = source[start:end]

    assert "place_protection" not in branch
    assert "cancel_all_orders" not in branch
    assert '"status": "flat"' in branch
    # The branch returns, so nothing below it can run in paper mode.
    assert branch.count("return") >= 2


def _module_source(module) -> str:
    """The module's source as it is on disk.

    Deliberately not inspect.getsource(some_method): that resolves through
    linecache and through whatever the attribute currently is, so a test
    elsewhere that patches the method makes these assertions read the patch.
    """
    from pathlib import Path

    return Path(module.__file__).read_text(encoding="utf-8")


def _function_source(module, name: str) -> str:
    """One `def name(` block, from the file, to its dedent."""
    source = _module_source(module)
    start = source.index(f"    def {name}(")
    rest = source[start + 1:]
    # The next line that starts a sibling definition at the same indent.
    for marker in ("\n    def ", "\n    @", "\nclass "):
        index = rest.find(marker)
        if index != -1:
            rest = rest[:index]
    return source[start] + rest
