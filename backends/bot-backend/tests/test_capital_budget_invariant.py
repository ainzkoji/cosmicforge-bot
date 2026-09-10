"""The capital budget invariant.

    committed margin  +  proposed margin  <=  capital budget

The worked example is `bot_a8117dc719fc`: `capital_budget=120`,
`allocation_value=120` fixed, `max_open_positions=2`. Nothing rejected that
configuration, because `resolve_effective_bot_policy` checks a single
allocation against the budget but never multiplies by the slot count, and the
executor's pre-trade check is against the *broker's* available balance, which
says nothing about how much of this bot's budget is already committed.

Two concurrent positions would have deployed 240 against a 120 budget.
"""
from __future__ import annotations

import pytest

from app.decision.reasons import ExecutionReason, RiskReason
from app.evidence.fill_bridge import project_fill
from app.evidence.writers import open_bot_run, open_runtime_session
from app.risk.capital_ledger import CapitalLedger, margin_for
from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import PAPER_FORWARD
from shared_lib.persistence.migrations import migrate

BOT = "bot_capital_test"
RUN_ID = "run_capital_test"
SYMBOL = "BTCUSDT"
BUDGET = 120.0


@pytest.fixture
def db():
    database = DB(":memory:")
    migrate(database)
    return database


class FakeContext:
    bot_instance_id = BOT
    user_id = "user_capital"
    broker_account_id = "brk_capital"
    broker_environment = "demo"
    market_type = "CRYPTO"
    effective_policy_hash = "policy_capital"


class FakeRunner:
    def __init__(self, db):
        self.db = db
        self.run_id = RUN_ID
        self.cycle_id = "cyc_capital"
        self.context = FakeContext()
        self.position_manager = None
        self._active_decision_ids = {}

    def _effective_execution_mode(self):
        return "paper"


@pytest.fixture
def runner(db):
    session = open_runtime_session(
        db, database_role="development", database_path=":memory:",
        process_execution_mode="paper", environment_name="capital-tests",
    )
    open_bot_run(
        db, run_id=RUN_ID, bot_instance_id=BOT, runtime_session_id=session,
        user_id="user_capital", policy_hash="policy_capital",
        provenance=PAPER_FORWARD, execution_mode="paper", broker_environment="demo",
    )
    return FakeRunner(db)


def ledger(db, budget=BUDGET):
    return CapitalLedger(db, bot_instance_id=BOT, capital_budget=budget)


def open_position(runner, db, *, position_id, qty, price, leverage=1.0):
    """Open a position through the real evidence path, at a known leverage."""
    decision_id = f"dec_{position_id}"
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO trading_decisions "
            "(decision_id, bot_instance_id, symbol, evaluated_at, leverage, complete) "
            "VALUES (?,?,?,?,?,1)",
            (decision_id, BOT, SYMBOL, "2026-01-01T00:00:00+00:00", leverage),
        )
    runner._active_decision_ids[SYMBOL] = decision_id
    project_fill(runner, db, {
        "symbol": SYMBOL, "side": "LONG", "action": "OPEN",
        "qty": qty, "price": price, "position_id": position_id,
    })


def close_position(runner, db, *, position_id, qty, price, action="CLOSE"):
    project_fill(runner, db, {
        "symbol": SYMBOL, "side": "LONG", "action": action,
        "qty": qty, "price": price, "position_id": position_id,
        "exit_reason": "TEST_CLOSE",
    })


# ══════════════════════════════════════════════════════════════════════════
# The invariant itself
# ══════════════════════════════════════════════════════════════════════════


def test_a_120_budget_cannot_approve_two_120_margin_positions(db, runner):
    """The exact configuration that was live and unflagged."""
    first = ledger(db).authorize(risk_notional=120.0, leverage=1.0, per_position_cap=120.0)
    assert first.approved
    assert first.approved_margin == pytest.approx(120.0)

    open_position(runner, db, position_id="pos_1", qty=1.2, price=100.0, leverage=1.0)
    assert ledger(db).committed_margin() == pytest.approx(120.0)

    second = ledger(db).authorize(risk_notional=120.0, leverage=1.0, per_position_cap=120.0)
    assert not second.approved
    assert second.reason == RiskReason.INSUFFICIENT_CAPITAL
    assert second.approved_margin == 0.0
    assert "fully committed" in second.detail


def test_remaining_capital_is_what_the_second_position_gets(db, runner):
    open_position(runner, db, position_id="pos_1", qty=0.5, price=100.0, leverage=1.0)
    assert ledger(db).available_capital() == pytest.approx(70.0)

    second = ledger(db).authorize(risk_notional=120.0, leverage=1.0, per_position_cap=120.0)
    assert second.approved
    assert second.approved_margin == pytest.approx(70.0)
    assert second.was_reduced

    # And the invariant holds on the total.
    assert ledger(db).committed_margin() + second.approved_margin <= BUDGET + 1e-9


def test_the_invariant_holds_regardless_of_the_configured_allocation(db, runner):
    """An allocation larger than the budget must not be able to breach it."""
    open_position(runner, db, position_id="pos_1", qty=1.0, price=100.0, leverage=1.0)

    # Operator has configured a 1,000 per-position allocation on a 120 budget.
    result = ledger(db).authorize(
        risk_notional=1_000.0, leverage=1.0, per_position_cap=1_000.0,
    )
    assert result.approved
    assert result.approved_margin == pytest.approx(20.0)
    assert result.committed_margin + result.approved_margin <= BUDGET + 1e-9


# ══════════════════════════════════════════════════════════════════════════
# Risk sizing stays authoritative
# ══════════════════════════════════════════════════════════════════════════


def test_a_fixed_allocation_never_inflates_a_risk_derived_size(db):
    """The allocation is a ceiling, not a target."""
    result = ledger(db).authorize(
        risk_notional=10.0,          # the risk layer says 10
        leverage=1.0,
        per_position_cap=120.0,      # the allocation says up to 120
    )
    assert result.approved
    assert result.approved_notional == pytest.approx(10.0), (
        "the allocation must not grow a risk-derived size"
    )


def test_the_chain_only_ever_shrinks(db, runner):
    open_position(runner, db, position_id="pos_1", qty=0.9, price=100.0, leverage=1.0)
    result = ledger(db).authorize(
        risk_notional=100.0, leverage=1.0, per_position_cap=50.0,
        max_exposure_notional=40.0,
    )
    sizes = [stage["notional"] for stage in result.stages]
    assert sizes == sorted(sizes, reverse=True), f"chain grew somewhere: {sizes}"
    assert result.approved_notional <= result.requested_notional


def test_a_non_positive_risk_size_is_rejected_not_defaulted(db):
    result = ledger(db).authorize(risk_notional=0.0, leverage=1.0, per_position_cap=120.0)
    assert not result.approved
    assert result.reason == RiskReason.STOP_INVALID


# ══════════════════════════════════════════════════════════════════════════
# Execution minimums
# ══════════════════════════════════════════════════════════════════════════


def test_a_size_under_the_exchange_minimum_is_rejected_explicitly(db, runner):
    """It must never be rounded up to reach the minimum."""
    open_position(runner, db, position_id="pos_1", qty=1.19, price=100.0, leverage=1.0)
    assert ledger(db).available_capital() == pytest.approx(1.0)

    result = ledger(db).authorize(
        risk_notional=50.0, leverage=1.0, per_position_cap=120.0, min_notional=5.0,
    )
    assert not result.approved
    assert result.reason == ExecutionReason.MIN_NOTIONAL
    assert result.approved_notional == pytest.approx(1.0)
    assert "Raising it would mean taking more risk" in result.detail


def test_a_size_that_meets_the_minimum_passes(db):
    result = ledger(db).authorize(
        risk_notional=6.0, leverage=1.0, per_position_cap=120.0, min_notional=5.0,
    )
    assert result.approved
    assert result.approved_notional == pytest.approx(6.0)


# ══════════════════════════════════════════════════════════════════════════
# Release and restart
# ══════════════════════════════════════════════════════════════════════════


def test_closing_a_position_restores_its_capital(db, runner):
    open_position(runner, db, position_id="pos_1", qty=1.2, price=100.0, leverage=1.0)
    assert ledger(db).available_capital() == pytest.approx(0.0)

    close_position(runner, db, position_id="pos_1", qty=1.2, price=110.0)

    assert ledger(db).committed_margin() == pytest.approx(0.0)
    assert ledger(db).available_capital() == pytest.approx(BUDGET)
    assert ledger(db).authorize(
        risk_notional=120.0, leverage=1.0, per_position_cap=120.0,
    ).approved


def test_a_partial_close_releases_exactly_its_share(db, runner):
    open_position(runner, db, position_id="pos_1", qty=1.0, price=100.0, leverage=1.0)
    assert ledger(db).committed_margin() == pytest.approx(100.0)

    close_position(runner, db, position_id="pos_1", qty=0.4, price=110.0,
                   action="PARTIAL_CLOSE")

    # 60% of the position remains, so 60% of the margin stays committed.
    assert ledger(db).committed_margin() == pytest.approx(60.0)
    assert ledger(db).available_capital() == pytest.approx(60.0)


def test_committed_capital_survives_a_restart(db, runner):
    """The ledger reads persisted rows, so a fresh object sees the same number."""
    open_position(runner, db, position_id="pos_1", qty=0.7, price=100.0, leverage=1.0)
    before = ledger(db).committed_margin()

    # A brand-new ledger and a brand-new runner: nothing carried in memory.
    fresh = CapitalLedger(db, bot_instance_id=BOT, capital_budget=BUDGET)
    assert fresh.committed_margin() == pytest.approx(before) == pytest.approx(70.0)
    assert fresh.available_capital() == pytest.approx(50.0)
    assert fresh.open_positions() == 1


def test_leverage_reduces_the_margin_a_position_commits(db, runner):
    open_position(runner, db, position_id="pos_1", qty=1.2, price=100.0, leverage=10.0)
    # 120 notional at 10x is 12 margin, not 120.
    assert ledger(db).committed_margin() == pytest.approx(12.0)
    assert ledger(db).available_capital() == pytest.approx(108.0)


def test_an_unreadable_ledger_fails_closed(db):
    """A ledger that cannot be read must not report "nothing committed"."""
    class Broken:
        def connect(self):
            raise RuntimeError("database gone")

    broken = CapitalLedger(Broken(), bot_instance_id=BOT, capital_budget=BUDGET)
    assert broken.committed_margin() == pytest.approx(BUDGET)
    assert broken.available_capital() == 0.0
    assert not broken.authorize(risk_notional=10.0, leverage=1.0).approved


def test_margin_for_is_one_definition(db):
    assert margin_for(2.0, 50.0, 1.0) == pytest.approx(100.0)
    assert margin_for(2.0, 50.0, 10.0) == pytest.approx(10.0)
    assert margin_for(-2.0, 50.0, 1.0) == pytest.approx(100.0)  # side-agnostic


# ══════════════════════════════════════════════════════════════════════════
# The wiring, so the invariant is actually reachable
# ══════════════════════════════════════════════════════════════════════════


def test_the_executor_authorises_capital_before_sizing():
    """execute_signal is a thin dispatcher; the pre-trade path is _execute_impl.

    The gate must come before the PAPER branch as well as before sizing. The
    earlier version of this test only checked the order relative to
    ``_size_qty``, and so stayed green while the paper branch returned before
    the ledger was ever consulted.
    """
    from app.execution import executor as executor_module

    source = _function_source(executor_module, "_execute_impl")
    gate = source.index("self._capital_gate(")
    assert gate < source.index('if effective_mode != "live":'), (
        "capital must be authorised before the paper/live split"
    )
    assert gate < source.index("self._size_qty("), (
        "capital must be authorised before the order is sized"
    )
    assert "_authorize_capital" in _function_source(executor_module, "_capital_gate")


def test_the_capital_check_precedes_the_broker_balance_check():
    """The bot's budget and the account's balance are different questions.

    Both must pass, and the bot's own constraint is applied to the notional
    first so the broker check sees the already-capped size.
    """
    from app.execution import executor as executor_module

    source = _function_source(executor_module, "_execute_impl")
    assert source.index("self._capital_gate(") < source.index("margin_required =")


def test_the_runner_teaches_the_executor_its_budget():
    import inspect

    from app.runner.runner import PaperRunner

    source = inspect.getsource(PaperRunner.__init__)
    assert "_capital_budget" in source
    assert "capital_budget" in source


def test_the_ledger_cannot_be_silently_disabled():
    """It needs three things wired, and all three are easy to drop.

    _authorize_capital returns None -- no opinion, trade proceeds -- when the
    budget, the bot id or the database reference is missing. That is the right
    behaviour for a bot with no configured budget and the wrong behaviour for a
    wiring regression, so the wiring is pinned here.
    """
    from app.runner import runner as runner_module

    source = _module_source(runner_module)
    assert "db=self.db" in source, "the executor must receive a database"
    assert "bot_instance_id=bot_id" in source
    assert "self.executor._capital_budget" in source


def test_no_budget_configured_means_no_ledger_opinion(db):
    """A bot without a budget is not silently blocked."""
    result = CapitalLedger(db, bot_instance_id=BOT, capital_budget=0.0).authorize(
        risk_notional=10.0, leverage=1.0,
    )
    assert not result.approved
    assert result.reason == RiskReason.CAPITAL_BUDGET_REQUIRED


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
