"""Fixed-amount allocation semantics.

``allocation_type=fixed_amount`` means ``allocation_value`` is the margin
capacity for one approved trade. It is not a total bot budget, is not divided
by ``max_open_positions``, and is not reduced by already-open committed margin.
"""
from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.decision.reasons import ExecutionReason, RiskReason
from app.evidence.fill_bridge import project_fill
from app.evidence.writers import open_bot_run, open_runtime_session
from app.execution.executor import BinanceExecutor
from app.risk.capital_ledger import CapitalAuthorization, CapitalLedger, margin_for
from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import PAPER_FORWARD
from shared_lib.persistence.migrations import migrate

BOT = "bot_capital_test"
RUN_ID = "run_capital_test"
SYMBOL = "BTCUSDT"
PER_TRADE = 120.0


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
        self._symbol_evidence = {}

    def _effective_execution_mode(self):
        return "paper"


@pytest.fixture
def runner(db):
    session = open_runtime_session(
        db,
        database_role="development",
        database_path=":memory:",
        process_execution_mode="paper",
        environment_name="capital-tests",
    )
    open_bot_run(
        db,
        run_id=RUN_ID,
        bot_instance_id=BOT,
        runtime_session_id=session,
        user_id="user_capital",
        policy_hash="policy_capital",
        provenance=PAPER_FORWARD,
        execution_mode="paper",
        broker_environment="demo",
    )
    return FakeRunner(db)


def ledger(db, budget=PER_TRADE):
    return CapitalLedger(db, bot_instance_id=BOT, capital_budget=budget)


def open_position(runner, db, *, position_id, qty, price, leverage=1.0, requested_qty=None):
    decision_id = f"dec_{position_id}"
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO trading_decisions "
            "(decision_id, bot_instance_id, symbol, evaluated_at, leverage, complete) "
            "VALUES (?,?,?,?,?,1)",
            (decision_id, BOT, SYMBOL, "2026-01-01T00:00:00+00:00", leverage),
        )
    runner._active_decision_ids[SYMBOL] = decision_id
    fill = {
        "symbol": SYMBOL,
        "side": "LONG",
        "action": "OPEN",
        "qty": qty,
        "price": price,
        "position_id": position_id,
        "leverage": leverage,
    }
    if requested_qty is not None:
        fill["requested_qty"] = requested_qty
    project_fill(runner, db, fill)


def close_position(runner, db, *, position_id, qty, price, action="CLOSE"):
    project_fill(
        runner,
        db,
        {
            "symbol": SYMBOL,
            "side": "LONG",
            "action": action,
            "qty": qty,
            "price": price,
            "position_id": position_id,
            "exit_reason": "TEST_CLOSE",
        },
    )


# Per-trade allocation semantics


def test_fixed_amount_120_means_120_per_trade(db):
    result = ledger(db).authorize(
        risk_notional=120.0, leverage=1.0, per_position_cap=120.0,
    )
    assert result.approved
    assert result.approved_margin == pytest.approx(120.0)
    assert result.available_capital == pytest.approx(120.0)


def test_one_open_120_trade_does_not_zero_next_trade_allocation(db, runner):
    open_position(runner, db, position_id="pos_1", qty=1.2, price=100.0, leverage=1.0)
    assert ledger(db).committed_margin() == pytest.approx(120.0)

    second = ledger(db).authorize(
        risk_notional=120.0, leverage=1.0, per_position_cap=120.0,
    )

    assert second.approved
    assert second.approved_margin == pytest.approx(120.0)
    assert second.committed_margin == pytest.approx(120.0)
    assert second.available_capital == pytest.approx(120.0)
    assert not any(s["stage"] == "capital_remaining" for s in second.stages)


def test_two_independent_trades_can_each_receive_120_when_other_gates_allow(db, runner):
    first = ledger(db).authorize(risk_notional=120.0, leverage=1.0, per_position_cap=120.0)
    assert first.approved
    open_position(runner, db, position_id="pos_1", qty=1.2, price=100.0, leverage=1.0)

    second = ledger(db).authorize(risk_notional=120.0, leverage=1.0, per_position_cap=120.0)
    assert second.approved
    assert second.approved_margin == pytest.approx(120.0)


def test_allocation_value_is_not_divided_by_max_open_positions(db):
    result = ledger(db).authorize(
        risk_notional=120.0,
        leverage=1.0,
        per_position_cap=120.0,
    )
    assert result.approved_margin == pytest.approx(120.0)
    assert result.approved_margin != pytest.approx(60.0)


def test_existing_committed_margin_is_not_subtracted_from_fixed_allocation(db, runner):
    open_position(runner, db, position_id="pos_1", qty=0.6, price=100.0, leverage=1.0)
    assert ledger(db).committed_margin() == pytest.approx(60.0)

    result = ledger(db).authorize(
        risk_notional=120.0,
        leverage=1.0,
        per_position_cap=120.0,
    )
    assert result.approved_margin == pytest.approx(120.0)


def test_bch_gala_regression_no_automatic_60_plus_60_split(db, runner):
    first = ledger(db).authorize(risk_notional=120.0, leverage=1.0, per_position_cap=120.0)
    assert first.approved_margin == pytest.approx(120.0)
    open_position(runner, db, position_id="bch_like", qty=120.0, price=1.0, leverage=1.0)

    second = ledger(db).authorize(risk_notional=120.0, leverage=1.0, per_position_cap=120.0)
    assert second.approved_margin == pytest.approx(120.0)


def test_bch_gala_like_60_size_requires_explicit_independent_risk_input(db):
    result = ledger(db).authorize(risk_notional=60.0, leverage=1.0, per_position_cap=120.0)
    assert result.approved
    assert result.approved_margin == pytest.approx(60.0)
    assert result.stages[0]["stage"] == "risk_derived"
    assert result.committed_margin == pytest.approx(0.0)


def test_risk_policy_can_independently_reduce_120(db):
    result = ledger(db).authorize(
        risk_notional=60.0,
        leverage=1.0,
        per_position_cap=120.0,
    )
    assert result.approved
    assert result.approved_margin == pytest.approx(60.0)
    assert result.stages[0]["stage"] == "risk_derived"


def test_explicit_portfolio_limit_can_independently_reduce_a_trade(db):
    result = ledger(db).authorize(
        risk_notional=120.0,
        leverage=1.0,
        per_position_cap=120.0,
        max_exposure_notional=80.0,
    )
    assert result.approved
    assert result.approved_margin == pytest.approx(80.0)
    assert any(s["stage"] == "exposure_limit" for s in result.stages)


def test_absent_explicit_portfolio_limit_no_aggregate_limit_is_invented(db, runner):
    open_position(runner, db, position_id="pos_1", qty=1.2, price=100.0, leverage=1.0)
    result = ledger(db).authorize(
        risk_notional=120.0,
        leverage=1.0,
        per_position_cap=120.0,
        max_exposure_notional=0.0,
    )
    assert result.approved
    assert result.approved_margin == pytest.approx(120.0)


def test_position_count_is_observed_separately_from_allocation(db, runner):
    open_position(runner, db, position_id="pos_1", qty=1.2, price=100.0)
    open_position(runner, db, position_id="pos_2", qty=1.2, price=100.0)
    assert ledger(db).open_positions() == 2
    assert ledger(db).authorize(
        risk_notional=120.0, leverage=1.0, per_position_cap=120.0,
    ).approved


# Execution minimums and leverage


def test_a_fixed_allocation_never_inflates_a_risk_derived_size(db):
    result = ledger(db).authorize(
        risk_notional=10.0,
        leverage=1.0,
        per_position_cap=120.0,
    )
    assert result.approved
    assert result.approved_notional == pytest.approx(10.0)


def test_the_chain_only_ever_shrinks(db):
    result = ledger(db).authorize(
        risk_notional=100.0,
        leverage=1.0,
        per_position_cap=50.0,
        max_exposure_notional=40.0,
    )
    sizes = [stage["notional"] for stage in result.stages]
    assert sizes == sorted(sizes, reverse=True)
    assert result.approved_notional <= result.requested_notional


def test_non_positive_risk_size_is_rejected_not_defaulted(db):
    result = ledger(db).authorize(risk_notional=0.0, leverage=1.0, per_position_cap=120.0)
    assert not result.approved
    assert result.reason == RiskReason.STOP_INVALID


def test_size_under_exchange_minimum_is_rejected_explicitly(db):
    result = ledger(db).authorize(
        risk_notional=4.0,
        leverage=1.0,
        per_position_cap=120.0,
        min_notional=5.0,
    )
    assert not result.approved
    assert result.reason == ExecutionReason.MIN_NOTIONAL
    assert result.approved_notional == pytest.approx(4.0)


def test_size_that_meets_minimum_passes(db):
    result = ledger(db).authorize(
        risk_notional=6.0,
        leverage=1.0,
        per_position_cap=120.0,
        min_notional=5.0,
    )
    assert result.approved


def test_leverage_converts_margin_to_notional_without_reinterpreting_120(db):
    result = ledger(db).authorize(
        risk_notional=1_200.0,
        leverage=10.0,
        per_position_cap=1_200.0,
    )
    assert result.approved
    assert result.approved_notional == pytest.approx(1_200.0)
    assert result.approved_margin == pytest.approx(120.0)


# Release, restart, and broker fill truth


def test_closing_position_releases_actual_committed_margin(db, runner):
    open_position(runner, db, position_id="pos_1", qty=1.2, price=100.0, leverage=1.0)
    assert ledger(db).committed_margin() == pytest.approx(120.0)

    close_position(runner, db, position_id="pos_1", qty=1.2, price=110.0)

    assert ledger(db).committed_margin() == pytest.approx(0.0)


def test_partial_close_releases_exactly_its_share(db, runner):
    open_position(runner, db, position_id="pos_1", qty=1.0, price=100.0, leverage=1.0)
    close_position(
        runner, db, position_id="pos_1", qty=0.4, price=110.0, action="PARTIAL_CLOSE",
    )
    assert ledger(db).committed_margin() == pytest.approx(60.0)


def test_committed_capital_survives_restart_without_capping_next_trade(db, runner):
    open_position(runner, db, position_id="pos_1", qty=0.7, price=100.0, leverage=1.0)
    fresh = CapitalLedger(db, bot_instance_id=BOT, capital_budget=PER_TRADE)
    assert fresh.committed_margin() == pytest.approx(70.0)
    assert fresh.available_capital() == pytest.approx(120.0)
    assert fresh.authorize(
        risk_notional=120.0, leverage=1.0, per_position_cap=120.0,
    ).approved


def test_an_unreadable_ledger_fails_closed(db):
    class Broken:
        def connect(self):
            raise RuntimeError("database gone")

    broken = CapitalLedger(Broken(), bot_instance_id=BOT, capital_budget=PER_TRADE)
    assert broken.committed_margin() == pytest.approx(PER_TRADE)
    assert broken.available_capital() == pytest.approx(PER_TRADE)
    assert not broken.authorize(risk_notional=10.0, leverage=1.0).approved


def test_margin_for_is_one_definition(db):
    assert margin_for(2.0, 50.0, 1.0) == pytest.approx(100.0)
    assert margin_for(2.0, 50.0, 10.0) == pytest.approx(10.0)
    assert margin_for(-2.0, 50.0, 1.0) == pytest.approx(100.0)


def test_actual_fill_slightly_above_120_is_recorded_truthfully(db, runner):
    open_position(runner, db, position_id="pos_truth", qty=120.03, price=1.0, leverage=1.0)
    assert ledger(db).committed_margin() == pytest.approx(120.03)
    result = ledger(db).authorize(risk_notional=120.0, leverage=1.0, per_position_cap=120.0)
    assert result.approved


def test_requested_qty_differs_from_executed_qty_without_margin_clamp(db, runner):
    open_position(
        runner, db, position_id="pos_partial", qty=30.0, requested_qty=60.0,
        price=1.0, leverage=1.0,
    )
    with db.connect() as conn:
        row = conn.execute(
            "SELECT requested_qty, broker_executed_qty, committed_margin "
            "FROM positions WHERE position_id='pos_partial'"
        ).fetchone()
    assert row["requested_qty"] == pytest.approx(60.0)
    assert row["broker_executed_qty"] == pytest.approx(30.0)
    assert row["committed_margin"] == pytest.approx(30.0)


def test_broker_fill_price_is_authoritative_for_committed_margin(db, runner):
    open_position(runner, db, position_id="pos_fill_price", qty=119.9, price=1.002)
    assert ledger(db).committed_margin() == pytest.approx(119.9 * 1.002)


# Per-trade pre-entry headroom


def _executor_for_headroom():
    ex = object.__new__(BinanceExecutor)
    ex.estimate_slippage = lambda _notional: 0.00015
    return ex


def _spec(*, step="0.001", min_qty="0.001", min_notional="5", contract_size="1"):
    return SimpleNamespace(
        step_size=Decimal(step),
        min_qty=Decimal(min_qty),
        min_notional=Decimal(min_notional),
        contract_size=Decimal(contract_size),
    )


def _auth(*, committed=0.0, available=120.0, approved_margin=120.0, leverage=1.0):
    return CapitalAuthorization(
        approved=True,
        reason=RiskReason.APPROVED,
        detail="test",
        capital_budget=PER_TRADE,
        committed_margin=committed,
        available_capital=available,
        requested_notional=approved_margin * leverage,
        approved_notional=approved_margin * leverage,
        requested_margin=approved_margin,
        approved_margin=approved_margin,
        leverage=leverage,
    )


def _gate(auth):
    return SimpleNamespace(authorization=auth, leverage=int(auth.leverage))


def test_per_trade_headroom_does_not_use_open_committed_margin():
    reserve = _executor_for_headroom()._pre_entry_capital_reserve(
        symbol=SYMBOL,
        qty=120.0,
        reference_price=1.0,
        leverage=1.0,
        capital_gate=_gate(_auth(committed=120.0, available=0.0, approved_margin=120.0)),
        spec=_spec(),
        submitted_notional=120.0,
    )
    assert reserve.approved
    assert reserve.max_margin == pytest.approx(120.0)
    assert reserve.qty > 119.0


def test_adverse_execution_price_movement_is_reserved_before_submission():
    reserve = _executor_for_headroom()._pre_entry_capital_reserve(
        symbol=SYMBOL,
        qty=12.0,
        reference_price=100.0,
        leverage=10.0,
        capital_gate=_gate(_auth(leverage=10.0)),
        spec=_spec(step="0.001"),
        submitted_notional=1_200.0,
    )
    adverse_price = 100.0 * 1.001
    assert reserve.approved
    assert margin_for(reserve.qty, adverse_price, 10.0) <= 120.0


def test_quantity_is_floored_to_step_after_headroom():
    reserve = _executor_for_headroom()._pre_entry_capital_reserve(
        symbol=SYMBOL,
        qty=1.234567,
        reference_price=100.0,
        leverage=1.0,
        capital_gate=_gate(_auth(approved_margin=120.0)),
        spec=_spec(step="0.01"),
        submitted_notional=123.4567,
    )
    assert reserve.approved
    assert Decimal(str(reserve.qty)) % Decimal("0.01") == 0


def test_coarse_step_size_can_only_reduce_not_round_up():
    reserve = _executor_for_headroom()._pre_entry_capital_reserve(
        symbol=SYMBOL,
        qty=1.2,
        reference_price=100.0,
        leverage=1.0,
        capital_gate=_gate(_auth()),
        spec=_spec(step="0.7", min_qty="0.7"),
        submitted_notional=120.0,
    )
    assert reserve.approved
    assert reserve.qty == pytest.approx(0.7)
    assert reserve.estimated_margin <= reserve.max_margin


def test_min_quantity_blocks_unexecutable_headroom_result():
    reserve = _executor_for_headroom()._pre_entry_capital_reserve(
        symbol=SYMBOL,
        qty=0.09,
        reference_price=100.0,
        leverage=1.0,
        capital_gate=_gate(_auth(approved_margin=9.0)),
        spec=_spec(step="0.01", min_qty="0.1", min_notional="5"),
        submitted_notional=9.0,
    )
    assert not reserve.approved
    assert reserve.reason == "qty_below_min_qty"


def test_min_notional_blocks_unexecutable_headroom_result():
    reserve = _executor_for_headroom()._pre_entry_capital_reserve(
        symbol=SYMBOL,
        qty=0.04,
        reference_price=100.0,
        leverage=1.0,
        capital_gate=_gate(_auth(approved_margin=4.0)),
        spec=_spec(step="0.01", min_qty="0.01", min_notional="5"),
        submitted_notional=4.0,
    )
    assert not reserve.approved
    assert reserve.reason == "below_min_notional"


def test_fee_and_slippage_headroom_reduce_executable_quantity():
    ex = _executor_for_headroom()
    ex.estimate_slippage = lambda _notional: 0.01
    reserve = ex._pre_entry_capital_reserve(
        symbol=SYMBOL,
        qty=120.0,
        reference_price=1.0,
        leverage=1.0,
        capital_gate=_gate(_auth()),
        spec=_spec(),
        submitted_notional=120.0,
    )
    assert reserve.approved
    assert reserve.buffer_rate >= 0.0104
    assert reserve.qty <= 118.765


def test_no_budget_configured_means_no_ledger_opinion(db):
    result = CapitalLedger(db, bot_instance_id=BOT, capital_budget=0.0).authorize(
        risk_notional=10.0,
        leverage=1.0,
    )
    assert not result.approved
    assert result.reason == RiskReason.CAPITAL_BUDGET_REQUIRED


def test_capital_tests_do_not_write_the_canonical_database(db):
    assert getattr(db, "path", ":memory:") == ":memory:"


# Wiring and regression guardrails


def test_the_executor_authorises_capital_before_sizing():
    from app.execution import executor as executor_module

    source = _function_source(executor_module, "_execute_impl")
    gate = source.index("self._capital_gate(")
    assert gate < source.index('if effective_mode != "live":')
    assert gate < source.index("self._size_qty(")
    assert "_authorize_capital" in _function_source(executor_module, "_capital_gate")


def test_the_capital_check_precedes_the_broker_balance_check():
    from app.execution import executor as executor_module

    source = _function_source(executor_module, "_execute_impl")
    assert source.index("self._capital_gate(") < source.index("margin_required =")


def test_the_runner_teaches_the_executor_its_allocation_context():
    import inspect

    from app.runner.runner import PaperRunner

    source = inspect.getsource(PaperRunner.__init__)
    assert "_capital_budget" in source
    assert "_allocation_value" in source
    assert "max_open_positions" not in source[source.index("self.executor._capital_budget") - 250:
                                               source.index("self.executor._capital_budget") + 250]


def test_pending_order_reservation_system_remains_wired():
    from app.execution import executor as executor_module

    source = _function_source(executor_module, "_execute_impl")
    assert "acquire_intent" in source
    assert "mark_failed" in source


def test_htf_bias_halving_is_an_independent_sizing_rule_not_slot_division():
    from app.runner import runner as runner_module

    source = _module_source(runner_module)
    assert "htf_opposed" in source
    assert "trade_usdt *= 0.5" in source
    htf_index = source.index("trade_usdt *= 0.5")
    window = source[max(0, htf_index - 500): htf_index + 200]
    assert "max_open_positions" not in window
    assert "open_positions" not in window


def _module_source(module) -> str:
    from pathlib import Path

    return Path(module.__file__).read_text(encoding="utf-8")


def _function_source(module, name: str) -> str:
    source = _module_source(module)
    start = source.index(f"    def {name}(")
    rest = source[start + 1:]
    for marker in ("\n    def ", "\n    @", "\nclass "):
        index = rest.find(marker)
        if index != -1:
            rest = rest[:index]
    return source[start] + rest
