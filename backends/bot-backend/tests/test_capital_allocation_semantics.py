"""Capital allocation semantics: ``fixed_amount`` is PER TRADE.

``allocation_type=fixed_amount, allocation_value=120`` means each approved trade
may use up to 120 USDT of margin. It is not a 120 total for the bot, it is not
divided by ``max_open_positions``, and open committed margin is never subtracted
from the next trade's allocation. Position count, account affordability, risk
sizing and an explicit portfolio limit are separate gates with their own reasons.

The regression is bot_a8117dc719fc on 2026-09-13 (capital_allocation=120, fixed
allocation 120, 7x, two slots). BCHUSDT was sized to 120 - 59.9635 (ATUSDT open)
= 60.04 margin and GALAUSDT to 120 - 60.0169 (BCHUSDT open) = 59.98, because the
old ledger enforced SUM(open committed) + proposed <= 120.
"""
from __future__ import annotations

import threading
import uuid
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.decision.reasons import ExecutionReason, RiskReason
from app.evidence.fill_bridge import project_fill
from app.evidence.writers import open_bot_run, open_runtime_session
from app.execution.executor import BinanceExecutor
from app.risk.capital_ledger import (
    ACCOUNT_RESERVATIONS,
    AccountMarginReservations,
    CapitalAuthorization,
    CapitalLedger,
    allocation_variance,
    capital_diagnostics,
    margin_for,
    per_trade_allocation_margin,
)
from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import PAPER_FORWARD
from shared_lib.persistence.migrations import migrate

BOT = "bot_capital_test"
RUN_ID = "run_capital_test"
SYMBOL = "BTCUSDT"
PER_TRADE = 120.0
LEV = 7.0
PRICE = 100.0


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
        db, database_role="development", database_path=":memory:",
        process_execution_mode="paper", environment_name="capital-tests",
    )
    open_bot_run(
        db, run_id=RUN_ID, bot_instance_id=BOT, runtime_session_id=session,
        user_id="user_capital", policy_hash="policy_capital",
        provenance=PAPER_FORWARD, execution_mode="paper", broker_environment="demo",
    )
    return FakeRunner(db)


def ledger(db, per_trade=PER_TRADE, portfolio=0.0):
    return CapitalLedger(
        db, bot_instance_id=BOT, per_trade_allocation=per_trade,
        portfolio_margin_limit=portfolio,
    )


def open_position(runner, db, *, position_id, qty, price, leverage=1.0,
                  requested_qty=None, symbol=SYMBOL):
    """Open a position through the real evidence path, at a known leverage."""
    decision_id = f"dec_{position_id}"
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO trading_decisions "
            "(decision_id, bot_instance_id, symbol, evaluated_at, leverage, complete) "
            "VALUES (?,?,?,?,?,1)",
            (decision_id, BOT, symbol, "2026-01-01T00:00:00+00:00", leverage),
        )
    runner._active_decision_ids[symbol] = decision_id
    fill = {
        "symbol": symbol, "side": "LONG", "action": "OPEN", "qty": qty,
        "price": price, "position_id": position_id, "leverage": leverage,
    }
    if requested_qty is not None:
        fill["requested_qty"] = requested_qty
    project_fill(runner, db, fill)


def close_position(runner, db, *, position_id, qty, price, action="CLOSE", fee=None):
    fill = {
        "symbol": SYMBOL, "side": "LONG", "action": action, "qty": qty,
        "price": price, "position_id": position_id, "exit_reason": "TEST_CLOSE",
    }
    if fee is not None:
        fill["fee"] = fee
    project_fill(runner, db, fill)


def _stage(result, name):
    return next(s for s in result.stages if s["stage"] == name)


# ══════════════════════════════════════════════════════════════════════════
# 1-7, 10: fixed_amount is a per-trade allocation
# ══════════════════════════════════════════════════════════════════════════


def test_fixed_amount_120_means_120_per_trade(db):
    result = ledger(db).authorize(risk_notional=PER_TRADE * LEV, leverage=LEV)
    assert result.approved
    assert result.approved_margin == pytest.approx(120.0)
    assert result.approved_notional == pytest.approx(840.0)
    obs = result.observability()
    assert obs["allocation_scope"] == "PER_TRADE"
    assert obs["configured_trade_allocation"] == pytest.approx(120.0)
    assert "capital_budget" not in obs and "available_capital" not in obs


def test_one_open_120_trade_does_not_zero_next_trade_allocation(db, runner):
    open_position(runner, db, position_id="pos_1", qty=1.2, price=100.0)
    assert ledger(db).committed_margin() == pytest.approx(120.0)

    second = ledger(db).authorize(risk_notional=120.0, leverage=1.0)

    assert second.approved
    assert second.approved_margin == pytest.approx(120.0)
    assert second.committed_margin == pytest.approx(120.0)
    assert all(s.get("reason") is None for s in second.stages)


def test_two_independent_trades_can_each_receive_120(db, runner):
    first = ledger(db).authorize(risk_notional=840.0, leverage=LEV)
    assert first.approved_margin == pytest.approx(120.0)
    open_position(runner, db, position_id="pos_1", qty=8.4, price=100.0, leverage=LEV)

    second = ledger(db).authorize(risk_notional=840.0, leverage=LEV)
    assert second.approved_margin == pytest.approx(120.0)
    open_position(runner, db, position_id="pos_2", qty=8.4, price=100.0, leverage=LEV)

    # ~240 across two trades does not breach a per-trade allocation.
    assert ledger(db).committed_margin() == pytest.approx(240.0)


def _policy_ctx(open_count, max_positions=2):
    from app.policy.policy_engine import PolicyContext

    return PolicyContext(
        symbol="ZZTESTUSDT", signal="BUY", position="NONE",
        open_positions_count=open_count, max_open_positions=max_positions,
        entry_price=100.0, equity=1_000.0, leverage=LEV,
        trade_amount_mode="fixed", trade_amount_value=PER_TRADE,
    )


def test_max_open_positions_blocks_trade_three_by_count_not_capital(db, runner):
    from app.policy.policy_engine import PolicyEngine, ReasonCode

    open_position(runner, db, position_id="pos_1", qty=8.4, price=100.0, leverage=LEV)
    open_position(runner, db, position_id="pos_2", qty=8.4, price=100.0, leverage=LEV)

    decision = PolicyEngine().evaluate(_policy_ctx(open_count=2))
    assert not decision.allowed
    assert decision.reason_code == ReasonCode.MAX_POSITIONS_REACHED

    # Capital is not what blocks trade #3: its allocation is still 120.
    assert ledger(db).authorize(risk_notional=840.0, leverage=LEV).approved_margin == pytest.approx(120.0)


def test_a_free_slot_is_not_blocked_by_position_count():
    from app.policy.policy_engine import PolicyEngine, ReasonCode

    decision = PolicyEngine().evaluate(_policy_ctx(open_count=1))
    assert decision.reason_code != ReasonCode.MAX_POSITIONS_REACHED


def _policy(max_open_positions):
    return SimpleNamespace(
        user_id="u", bot_instance_id=BOT, broker_account_id="brk", symbols=("BTCUSDT",),
        strategy_id="master_ensemble", execution_mode="paper", timeframe="15m",
        market_type="CRYPTO", risk_level="balanced", max_leverage=LEV, max_daily_loss=6.0,
        max_open_positions=max_open_positions, max_daily_trades=6, min_risk_reward=1.8,
        position_allocation_type="fixed_amount", position_allocation_value=PER_TRADE,
        capital_budget=PER_TRADE, minimum_notional=5.0, stop_loss_fraction=0.02,
        broker_environment="demo", risk_per_trade=0.0025, max_weekly_drawdown=5.0,
        max_monthly_drawdown=10.0, higher_timeframe="4h", universe_mode="ALLOWLIST",
        policy_hash="h",
    )


@pytest.mark.parametrize("slots", [1, 2, 5])
def test_allocation_value_is_not_divided_by_max_open_positions(slots):
    from app.runner.bot_context import BotRunContext

    context = BotRunContext.from_effective_policy(_policy(slots), {})
    assert context.max_open_positions == slots
    assert context.trade_usdt_per_order == pytest.approx(120.0)
    assert per_trade_allocation_margin("fixed_amount", 120.0, 120.0) == pytest.approx(120.0)


def test_existing_committed_margin_is_not_subtracted_from_fixed_allocation(db, runner):
    open_position(runner, db, position_id="pos_1", qty=0.6, price=100.0)
    assert ledger(db).committed_margin() == pytest.approx(60.0)
    result = ledger(db).authorize(risk_notional=120.0, leverage=1.0)
    assert result.approved_margin == pytest.approx(120.0)


def test_risk_policy_can_independently_reduce_120(db):
    result = ledger(db).authorize(risk_notional=60.0 * LEV, leverage=LEV)
    assert result.approved
    assert result.approved_margin == pytest.approx(60.0)
    # The reduction is the risk layer's; the allocation did not bind.
    assert result.stages[0]["stage"] == "risk_derived"
    assert "reason" not in _stage(result, "per_trade_allocation")


def test_allocation_caps_a_larger_risk_size_with_an_explicit_reason(db):
    result = ledger(db).authorize(risk_notional=2_000.0, leverage=LEV)
    assert result.approved_margin == pytest.approx(120.0)
    assert _stage(result, "per_trade_allocation")["reason"] == RiskReason.PER_TRADE_ALLOCATION_LIMIT


def test_absent_an_explicit_portfolio_limit_none_is_invented(db, runner):
    open_position(runner, db, position_id="pos_1", qty=8.4, price=100.0, leverage=LEV)
    open_position(runner, db, position_id="pos_2", qty=8.4, price=100.0, leverage=LEV)
    result = ledger(db).authorize(risk_notional=840.0, leverage=LEV)
    assert result.approved_margin == pytest.approx(120.0)
    assert result.observability()["explicit_portfolio_limit"] is None
    assert BinanceExecutor(client=MagicMock(), bot_instance_id=BOT)._portfolio_margin_limit == 0.0

    diag = capital_diagnostics(
        db, bot_instance_id=BOT, allocation_type="fixed_amount", allocation_value=120.0,
        capital_allocation=120.0, max_open_positions=2,
    )
    assert diag["allocation_scope"] == "PER_TRADE"
    assert diag["capital_allocation_is_aggregate_limit"] is False
    assert diag["explicit_portfolio_limit"] is None
    assert diag["aggregate_committed_margin"] == pytest.approx(240.0)
    assert diag["next_trade_configured_allocation"] == pytest.approx(120.0)
    assert diag["position_slot_available"] is False


# ══════════════════════════════════════════════════════════════════════════
# 9: an explicit portfolio limit is a separate gate
# ══════════════════════════════════════════════════════════════════════════


def test_explicit_portfolio_limit_can_reduce_a_trade(db, runner):
    open_position(runner, db, position_id="pos_1", qty=1.2, price=100.0)
    result = ledger(db, portfolio=200.0).authorize(risk_notional=120.0, leverage=1.0)
    assert result.approved
    assert result.approved_margin == pytest.approx(80.0)
    assert _stage(result, "portfolio_limit")["reason"] == RiskReason.PORTFOLIO_EXPOSURE_LIMIT


def test_explicit_portfolio_limit_can_block_a_trade(db, runner):
    open_position(runner, db, position_id="pos_1", qty=2.0, price=100.0)
    result = ledger(db, portfolio=200.0).authorize(risk_notional=120.0, leverage=1.0)
    assert not result.approved
    assert result.reason == RiskReason.PORTFOLIO_EXPOSURE_LIMIT


# ══════════════════════════════════════════════════════════════════════════
# The BCHUSDT / GALAUSDT regression
# ══════════════════════════════════════════════════════════════════════════


def test_bch_regression_open_atusdt_does_not_shrink_bch_to_the_remainder(db, runner):
    open_position(runner, db, position_id="at", symbol="ATUSDT", qty=2789.0,
                  price=0.1505, leverage=LEV)
    assert ledger(db).committed_margin() == pytest.approx(59.9635)

    bch = ledger(db).authorize(risk_notional=840.0, leverage=LEV)

    old_remainder_notional = (120.0 - 59.9635) * LEV  # 420.2555, what was sized
    assert bch.approved_margin == pytest.approx(120.0)
    assert bch.approved_notional == pytest.approx(840.0)
    assert bch.approved_notional > old_remainder_notional


def test_gala_regression_open_bch_does_not_shrink_gala_to_the_remainder(db, runner):
    open_position(runner, db, position_id="bch", symbol="BCHUSDT", qty=1.86,
                  price=225.87, leverage=LEV)
    assert ledger(db).committed_margin() == pytest.approx(60.01688571428572)

    gala = ledger(db).authorize(risk_notional=840.0, leverage=LEV)
    assert gala.approved_margin == pytest.approx(120.0)


def test_a_60_margin_size_requires_an_explicit_independent_risk_input(db):
    result = ledger(db).authorize(risk_notional=420.0, leverage=LEV)
    assert result.approved_margin == pytest.approx(60.0)
    assert result.stages[0] == {
        "stage": "risk_derived", "notional": 420.0, "margin": 60.0,
        "note": "authoritative ceiling",
    }
    assert all("reason" not in s for s in result.stages)


class MarketOnly:
    def get_prices(self, symbols):
        return {s: PRICE for s in symbols}

    def get_ticker(self, symbol):
        return {"lastPrice": PRICE}


def test_paper_executor_sizes_bch_at_its_own_120_with_atusdt_open(db, runner):
    open_position(runner, db, position_id="at", symbol="ATUSDT", qty=2789.0,
                  price=0.1505, leverage=LEV)
    ex = BinanceExecutor(client=MarketOnly(), execution_mode="paper", bot_instance_id=BOT, db=db)
    ex._capital_budget = PER_TRADE
    ex._allocation_type = "fixed_amount"
    ex._allocation_value = PER_TRADE

    result = ex.execute_signal("BCHUSDT", "SELL", 840.0, leverage_override=int(LEV))

    assert result.success, result.error
    assert result.details["capital"]["approved_margin"] == pytest.approx(120.0)
    assert result.details["capital"]["aggregate_committed_margin"] == pytest.approx(59.9635)


# ══════════════════════════════════════════════════════════════════════════
# 11-16: execution minimums, leverage, per-trade headroom
# ══════════════════════════════════════════════════════════════════════════


def test_a_fixed_allocation_never_inflates_a_risk_derived_size(db):
    result = ledger(db).authorize(risk_notional=10.0, leverage=1.0)
    assert result.approved
    assert result.approved_notional == pytest.approx(10.0)


def test_the_chain_only_ever_shrinks(db):
    result = ledger(db, per_trade=50.0).authorize(
        risk_notional=100.0, leverage=1.0, max_exposure_notional=40.0,
    )
    sizes = [stage["notional"] for stage in result.stages]
    assert sizes == sorted(sizes, reverse=True)
    assert result.approved_notional <= result.requested_notional


def test_non_positive_risk_size_is_rejected_not_defaulted(db):
    result = ledger(db).authorize(risk_notional=0.0, leverage=1.0)
    assert not result.approved
    assert result.reason == RiskReason.STOP_INVALID


def test_size_under_exchange_minimum_is_rejected_explicitly(db):
    result = ledger(db).authorize(risk_notional=4.0, leverage=1.0, min_notional=5.0)
    assert not result.approved
    assert result.reason == ExecutionReason.MIN_NOTIONAL
    assert result.approved_notional == pytest.approx(4.0)


def test_leverage_converts_margin_to_notional_without_reinterpreting_120(db):
    result = ledger(db).authorize(risk_notional=5_000.0, leverage=10.0)
    assert result.approved_notional == pytest.approx(1_200.0)
    assert result.approved_margin == pytest.approx(120.0)


def _executor_for_headroom():
    ex = object.__new__(BinanceExecutor)
    ex.estimate_slippage = lambda _notional: 0.00015
    return ex


def _spec(*, step="0.001", min_qty="0.001", min_notional="5", contract_size="1"):
    return SimpleNamespace(
        step_size=Decimal(step), min_qty=Decimal(min_qty),
        min_notional=Decimal(min_notional), contract_size=Decimal(contract_size),
    )


def _auth(*, committed=0.0, approved_margin=120.0, leverage=1.0):
    return CapitalAuthorization(
        approved=True, reason=RiskReason.APPROVED, detail="test",
        per_trade_allocation=PER_TRADE, committed_margin=committed, open_positions=0,
        portfolio_margin_limit=0.0, requested_notional=approved_margin * leverage,
        approved_notional=approved_margin * leverage, requested_margin=approved_margin,
        approved_margin=approved_margin, leverage=leverage,
    )


def _gate(auth):
    return SimpleNamespace(authorization=auth, leverage=int(auth.leverage))


def _reserve(qty, ref, lev, auth, spec, notional, ex=None):
    return (ex or _executor_for_headroom())._pre_entry_capital_reserve(
        symbol=SYMBOL, qty=qty, reference_price=ref, leverage=lev,
        capital_gate=_gate(auth), spec=spec, submitted_notional=notional,
    )


def test_per_trade_headroom_does_not_use_open_committed_margin():
    reserve = _reserve(120.0, 1.0, 1.0, _auth(committed=240.0), _spec(), 120.0)
    assert reserve.approved
    assert reserve.max_margin == pytest.approx(120.0)
    assert reserve.qty > 119.0


def test_adverse_execution_price_movement_is_reserved_before_submission():
    reserve = _reserve(12.0, 100.0, 10.0, _auth(leverage=10.0), _spec(), 1_200.0)
    assert reserve.approved
    assert margin_for(reserve.qty, 100.0 * 1.001, 10.0) <= 120.0


def test_quantity_is_floored_to_step_after_headroom():
    reserve = _reserve(1.234567, 100.0, 1.0, _auth(), _spec(step="0.01"), 123.4567)
    assert reserve.approved
    assert Decimal(str(reserve.qty)) % Decimal("0.01") == 0


def test_coarse_step_size_can_only_reduce_not_round_up():
    reserve = _reserve(1.2, 100.0, 1.0, _auth(), _spec(step="0.7", min_qty="0.7"), 120.0)
    assert reserve.approved
    assert reserve.qty == pytest.approx(0.7)
    assert reserve.estimated_margin <= reserve.max_margin


def test_min_quantity_blocks_unexecutable_headroom_result():
    reserve = _reserve(0.09, 100.0, 1.0, _auth(approved_margin=9.0),
                       _spec(step="0.01", min_qty="0.1"), 9.0)
    assert not reserve.approved
    assert reserve.reason == "qty_below_min_qty"


def test_min_notional_blocks_unexecutable_headroom_result():
    reserve = _reserve(0.04, 100.0, 1.0, _auth(approved_margin=4.0),
                       _spec(step="0.01", min_qty="0.01"), 4.0)
    assert not reserve.approved
    assert reserve.reason == "below_min_notional"


def test_fee_and_slippage_headroom_reduce_executable_quantity():
    ex = _executor_for_headroom()
    ex.estimate_slippage = lambda _notional: 0.01
    reserve = _reserve(120.0, 1.0, 1.0, _auth(), _spec(), 120.0, ex=ex)
    assert reserve.approved
    assert reserve.buffer_rate >= 0.0104
    assert reserve.qty <= 118.765


# ══════════════════════════════════════════════════════════════════════════
# 8, 19-21: account affordability and in-flight reservations
# ══════════════════════════════════════════════════════════════════════════


def test_account_affordability_reduces_a_trade_it_cannot_fully_fund():
    reg = AccountMarginReservations()
    res = reg.reserve("acct", available_balance=100.0, requested_margin=120.0,
                      safety_buffer=0.95, min_available=5.0)
    assert res.approved and res.reduced
    assert res.reserved_margin == pytest.approx(95.0)
    assert res.reason == RiskReason.MARGIN_INSUFFICIENT


def test_account_affordability_blocks_when_the_account_cannot_fund_it():
    reg = AccountMarginReservations()
    res = reg.reserve("acct", available_balance=4.0, requested_margin=120.0, min_available=5.0)
    assert not res.approved
    assert res.reason == RiskReason.ACCOUNT_AVAILABLE_BALANCE_INSUFFICIENT
    assert res.token is None and reg.pending_margin("acct") == 0.0


def test_pending_reservations_cannot_double_use_account_margin():
    reg = AccountMarginReservations()
    first = reg.reserve("acct", available_balance=150.0, requested_margin=120.0)
    # The broker snapshot has not caught up: it still shows 150 free.
    second = reg.reserve("acct", available_balance=150.0, requested_margin=120.0)
    assert first.reserved_margin == pytest.approx(120.0)
    assert second.pending_margin == pytest.approx(120.0)
    assert second.reserved_margin == pytest.approx(30.0)
    # A different account is unaffected.
    assert reg.reserve("other", available_balance=150.0, requested_margin=120.0).reserved_margin == 120.0


def test_concurrent_reservations_never_overcommit_the_account():
    reg = AccountMarginReservations()
    barrier = threading.Barrier(16)
    results = []

    def worker():
        barrier.wait()
        results.append(reg.reserve("acct", available_balance=500.0,
                                   requested_margin=120.0, min_available=5.0))

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(r.reserved_margin for r in results if r.approved) <= 500.0 + 1e-9
    assert reg.pending_margin("acct") <= 500.0 + 1e-9


def test_failed_or_cancelled_order_releases_its_reservation():
    reg = AccountMarginReservations()
    res = reg.reserve("acct", available_balance=500.0, requested_margin=120.0)
    reg.release(res.token)
    assert reg.pending_margin("acct") == 0.0


def test_partial_fill_settles_to_executed_margin_then_expires():
    now = [1_000.0]
    reg = AccountMarginReservations(settle_seconds=10.0, clock=lambda: now[0])
    res = reg.reserve("acct", available_balance=500.0, requested_margin=120.0)
    reg.settle(res.token, executed_margin=60.0)
    assert reg.pending_margin("acct") == pytest.approx(60.0)
    reg.release(res.token)  # a settled order exists at the broker: not released early
    assert reg.pending_margin("acct") == pytest.approx(60.0)
    now[0] += 11.0
    assert reg.pending_margin("acct") == 0.0


def test_an_abandoned_reservation_expires_instead_of_blocking_forever():
    now = [0.0]
    reg = AccountMarginReservations(max_age_seconds=120.0, clock=lambda: now[0])
    reg.reserve("acct", available_balance=500.0, requested_margin=120.0)
    now[0] += 121.0
    assert reg.pending_margin("acct") == 0.0


# ── The live executor path ──────────────────────────────────────────────────


def _live_client(*, available="1000.0", account_exc=None, place_exc=None, fill_fraction=1.0):
    client = MagicMock()
    client.get_position_info.return_value = {"positionAmt": "0.0"}
    if account_exc is not None:
        client.account.side_effect = account_exc
    else:
        client.account.return_value = {
            "availableBalance": available, "totalWalletBalance": available,
            "totalMaintMargin": "0.0", "totalInitialMargin": "0.0",
        }
    client.get_prices.return_value = {SYMBOL: PRICE}
    client.get_klines.return_value = [[0, 0, 0, 0, 0, 0, 1_700_000_000_000]]

    def place_order(req):
        if place_exc is not None:
            raise place_exc
        filled = float(req.qty) * fill_fraction
        return SimpleNamespace(
            broker_order_id="E-1", avg_fill_price=PRICE, qty_filled=filled,
            model_dump=lambda: {"orderId": "E-1", "status": "FILLED",
                                "executedQty": str(filled), "avgPrice": str(PRICE),
                                "updateTime": 1_700_000_000_000},
        )

    client.place_order.side_effect = place_order
    client.place_protection.return_value = SimpleNamespace(
        status="success", sl_order_id="SL-1", tp_order_id="TP-1",
        model_dump=lambda: {"status": "success"},
    )
    return client


def _live_executor(db, client, account_key):
    ex = BinanceExecutor(client=client, execution_mode="live", live_symbols=[SYMBOL],
                         bot_instance_id=BOT, db=db)
    ex.run_id = "run-capital"
    ex._capital_budget = PER_TRADE
    ex._allocation_type = "fixed_amount"
    ex._allocation_value = PER_TRADE
    ex._max_notional_per_symbol = PER_TRADE * LEV * 1.20
    ex._broker_account_id = account_key
    sized = {}

    def size(symbol, budget, lev_mult, sl_price=0.0, leverage_override=None):
        sized["budget"] = budget
        return budget / PRICE, {"price": PRICE, "leverage": int(LEV)}

    ex._size_qty = size
    return ex, sized


def _run_live(ex):
    with patch("app.execution.executor.time.time", return_value=1_700_000_000.0):
        return ex.execute_signal(SYMBOL, "BUY", PER_TRADE * LEV, sl_price=95.0, tp_price=110.0,
                                 current_equity=1_000.0, leverage_override=int(LEV))


def test_live_entry_uses_its_full_120_when_the_account_affords_it(db):
    key = f"acct_{uuid.uuid4().hex}"
    ex, sized = _live_executor(db, _live_client(), key)
    result = _run_live(ex)
    assert result.status == "ORDER_PLACED", result.error
    assert sized["budget"] == pytest.approx(840.0)
    assert result.details["capital"]["approved_margin"] == pytest.approx(120.0)
    assert result.details["account_affordability"]["reduced"] is False


def test_live_entry_is_reduced_by_real_account_affordability(db):
    key = f"acct_{uuid.uuid4().hex}"
    ex, sized = _live_executor(db, _live_client(available="50.0"), key)
    result = _run_live(ex)
    assert result.status == "ORDER_PLACED", result.error
    assert sized["budget"] == pytest.approx(50.0 * 0.95 * LEV)
    affordability = result.details["account_affordability"]
    assert affordability["reduced"] is True
    assert affordability["reason"] == RiskReason.MARGIN_INSUFFICIENT


def test_live_entry_is_blocked_when_the_account_cannot_fund_it(db):
    key = f"acct_{uuid.uuid4().hex}"
    client = _live_client(available="3.0")
    ex, _ = _live_executor(db, client, key)
    result = _run_live(ex)
    assert result.status == "INSUFFICIENT_MARGIN"
    assert result.details["reason_code"] == RiskReason.ACCOUNT_AVAILABLE_BALANCE_INSUFFICIENT
    client.place_order.assert_not_called()


def test_an_unreadable_account_blocks_the_entry_fail_closed(db):
    key = f"acct_{uuid.uuid4().hex}"
    client = _live_client(account_exc=RuntimeError("account endpoint down"))
    ex, _ = _live_executor(db, client, key)
    result = _run_live(ex)
    assert result.status == "INSUFFICIENT_MARGIN"
    assert result.details["reason_code"] == ExecutionReason.MARGIN_PREFLIGHT_FAILED
    client.place_order.assert_not_called()
    assert ACCOUNT_RESERVATIONS.pending_margin(key) == 0.0


def test_another_in_flight_entry_on_the_same_account_is_not_spent_twice(db):
    key = f"acct_{uuid.uuid4().hex}"
    held = ACCOUNT_RESERVATIONS.reserve(key, available_balance=150.0, requested_margin=100.0)
    try:
        ex, sized = _live_executor(db, _live_client(available="150.0"), key)
        result = _run_live(ex)
        assert result.status == "ORDER_PLACED", result.error
        # 150 free at the broker, 100 already reserved by another entry: 50 x 0.95.
        assert sized["budget"] == pytest.approx(50.0 * 0.95 * LEV)
    finally:
        ACCOUNT_RESERVATIONS.release(held.token)


def test_a_rejected_live_order_releases_its_reservation(db):
    key = f"acct_{uuid.uuid4().hex}"
    client = _live_client(place_exc=Exception('{"code":-2019,"msg":"Margin is insufficient."}'))
    ex, _ = _live_executor(db, client, key)
    result = _run_live(ex)
    assert result.status == "INSUFFICIENT_MARGIN"
    assert ACCOUNT_RESERVATIONS.pending_margin(key) == 0.0


def test_a_partial_live_fill_settles_the_reservation_to_executed_margin(db):
    key = f"acct_{uuid.uuid4().hex}"
    ex, _ = _live_executor(db, _live_client(fill_fraction=0.5), key)
    result = _run_live(ex)
    assert result.status == "ORDER_PLACED", result.error
    executed = result.details["filled_qty"] * PRICE / LEV
    assert executed < 60.1
    assert ACCOUNT_RESERVATIONS.pending_margin(key) == pytest.approx(executed)
    assert result.details["allocation_variance"]["actual_committed_margin"] == pytest.approx(executed)


# ══════════════════════════════════════════════════════════════════════════
# 17-18, 22-25: broker fill truth, per-trade variance, release
# ══════════════════════════════════════════════════════════════════════════


def test_actual_fill_slightly_above_120_is_recorded_truthfully(db, runner):
    open_position(runner, db, position_id="pos_truth", qty=120.03, price=1.0)
    assert ledger(db).committed_margin() == pytest.approx(120.03)
    variance = allocation_variance(
        configured_trade_allocation=120.0, pre_trade_target_margin=119.9,
        actual_committed_margin=120.03,
    )
    assert variance["allocation_variance"] == pytest.approx(0.03)
    assert variance["allocation_variance_pct"] == pytest.approx(0.025)
    assert variance["exceeds_trade_allocation"] is True


def test_fill_variance_is_per_trade_not_an_aggregate_breach(db, runner):
    open_position(runner, db, position_id="pos_a", qty=120.0, price=1.0)
    open_position(runner, db, position_id="pos_b", qty=120.03, price=1.0)
    assert ledger(db).committed_margin() == pytest.approx(240.03)
    variance = allocation_variance(
        configured_trade_allocation=120.0, pre_trade_target_margin=119.9,
        actual_committed_margin=120.03,
    )
    assert variance["allocation_variance"] == pytest.approx(0.03)
    assert ledger(db).authorize(risk_notional=120.0, leverage=1.0).approved_margin == pytest.approx(120.0)


def test_requested_qty_differs_from_executed_qty_without_margin_clamp(db, runner):
    open_position(runner, db, position_id="pos_partial", qty=30.0, requested_qty=60.0, price=1.0)
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


def test_fees_remain_recorded(db, runner):
    open_position(runner, db, position_id="pos_fee", qty=1.2, price=100.0)
    close_position(runner, db, position_id="pos_fee", qty=1.2, price=101.0, fee=0.0484)
    with db.connect() as conn:
        fees = conn.execute("SELECT fees FROM positions WHERE position_id='pos_fee'").fetchone()[0]
    assert fees == pytest.approx(0.0484)


def test_closing_position_releases_actual_committed_margin(db, runner):
    open_position(runner, db, position_id="pos_1", qty=1.2, price=100.0)
    assert ledger(db).committed_margin() == pytest.approx(120.0)
    close_position(runner, db, position_id="pos_1", qty=1.2, price=110.0)
    assert ledger(db).committed_margin() == pytest.approx(0.0)


def test_partial_close_releases_exactly_its_share(db, runner):
    open_position(runner, db, position_id="pos_1", qty=1.0, price=100.0)
    close_position(runner, db, position_id="pos_1", qty=0.4, price=110.0, action="PARTIAL_CLOSE")
    assert ledger(db).committed_margin() == pytest.approx(60.0)


def test_committed_margin_survives_restart_without_capping_next_trade(db, runner):
    open_position(runner, db, position_id="pos_1", qty=0.7, price=100.0)
    fresh = CapitalLedger(db, bot_instance_id=BOT, per_trade_allocation=PER_TRADE)
    assert fresh.committed_margin() == pytest.approx(70.0)
    assert fresh.open_positions() == 1
    assert fresh.authorize(risk_notional=120.0, leverage=1.0).approved_margin == pytest.approx(120.0)


def test_an_unreadable_ledger_fails_closed(db):
    class Broken:
        def connect(self):
            raise RuntimeError("database gone")

    broken = CapitalLedger(Broken(), bot_instance_id=BOT, per_trade_allocation=PER_TRADE)
    assert broken.committed_margin() is None  # unknown, never reported as zero
    result = broken.authorize(risk_notional=10.0, leverage=1.0)
    assert not result.approved
    assert result.reason == RiskReason.CAPITAL_LEDGER_UNAVAILABLE


def test_no_per_trade_allocation_configured_is_rejected(db):
    result = CapitalLedger(db, bot_instance_id=BOT, per_trade_allocation=0.0).authorize(
        risk_notional=10.0, leverage=1.0,
    )
    assert not result.approved
    assert result.reason == RiskReason.CAPITAL_BUDGET_REQUIRED


def test_margin_for_is_one_definition():
    assert margin_for(2.0, 50.0, 1.0) == pytest.approx(100.0)
    assert margin_for(2.0, 50.0, 10.0) == pytest.approx(10.0)
    assert margin_for(-2.0, 50.0, 1.0) == pytest.approx(100.0)


def test_capital_tests_do_not_write_the_canonical_database(db):
    assert getattr(db, "path", ":memory:") == ":memory:"


# ══════════════════════════════════════════════════════════════════════════
# Wiring and regression guardrails
# ══════════════════════════════════════════════════════════════════════════


def test_the_executor_authorises_capital_before_sizing():
    from app.execution import executor as executor_module

    source = _function_source(executor_module, "_execute_impl_body")
    gate = source.index("self._capital_gate(")
    assert gate < source.index('if effective_mode != "live":')
    assert gate < source.index("self._size_qty(")
    assert "_authorize_capital" in _function_source(executor_module, "_capital_gate")


def test_account_reservation_precedes_order_submission_and_is_always_ended():
    from app.execution import executor as executor_module

    body = _function_source(executor_module, "_execute_impl_body")
    assert body.index("self._capital_gate(") < body.index("ACCOUNT_RESERVATIONS.reserve(")
    assert body.index("ACCOUNT_RESERVATIONS.reserve(") < body.index("self.client.place_order(")
    assert "ACCOUNT_RESERVATIONS.release(" in _function_source(executor_module, "_execute_impl")
    assert "Proceeding cautiously" not in _module_source(executor_module)


def test_no_aggregate_budget_survives_in_the_ledger():
    from app.risk import capital_ledger as ledger_module

    source = _module_source(ledger_module)
    code = source.split('"""', 2)[2]  # skip the module docstring's worked example
    assert "capital_remaining" not in code
    assert "fully committed" not in code
    assert "capital_budget" not in code


def test_the_runner_teaches_the_executor_its_allocation_context():
    import inspect

    from app.runner.runner import PaperRunner

    source = inspect.getsource(PaperRunner.__init__)
    assert "_allocation_value" in source
    assert "_broker_account_id" in source
    anchor = source.index("self.executor._capital_budget")
    assert "max_open_positions" not in source[anchor - 250: anchor + 250]


def test_entry_protection_remains_wired():
    from app.execution import executor as executor_module

    source = _function_source(executor_module, "_execute_impl_body")
    assert "acquire_intent" in source
    assert "mark_failed" in source


def test_htf_bias_halving_is_an_independent_sizing_rule_not_slot_division():
    from app.runner import runner as runner_module

    source = _module_source(runner_module)
    htf_index = source.index("trade_usdt *= 0.5")
    window = source[max(0, htf_index - 500): htf_index + 200]
    assert "htf_opposed" in window
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
