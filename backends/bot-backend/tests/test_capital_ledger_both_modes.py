"""One capital policy governs paper and live, and it fails closed.

The audited defect: ``BinanceExecutor._execute_impl`` handed paper entries to
``PaperExecutor`` before ``_authorize_capital`` was ever reached, so paper trading
ignored ``committed + proposed <= budget``. And when authorisation raised, it
returned None, which the caller read as "no opinion -- proceed".

The worked example is the live bot's configuration, unchanged here: budget 120
USDT, fixed allocation 120 USDT per position, two slots. After one 120-USDT
commitment the second must be rejected -- in paper and in live alike.
"""
from __future__ import annotations

import pytest

from app.decision.reasons import RiskReason
from app.evidence.fill_bridge import project_fill
from app.evidence.writers import open_bot_run, open_runtime_session
from app.execution.executor import BinanceExecutor, ExchangeError
from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import PAPER_FORWARD
from shared_lib.persistence.migrations import migrate

BOT = "bot_capital_modes"
RUN_ID = "run_capital_modes"
BUDGET = 120.0
PRICE = 100.0


@pytest.fixture
def db():
    database = DB(":memory:")
    migrate(database)
    session = open_runtime_session(
        database, database_role="development", database_path=":memory:",
        process_execution_mode="paper", environment_name="capital-modes",
    )
    open_bot_run(
        database, run_id=RUN_ID, bot_instance_id=BOT, runtime_session_id=session,
        user_id="user_capital", policy_hash="policy_capital", provenance=PAPER_FORWARD,
        execution_mode="paper", broker_environment="demo",
    )
    return database


class MarketOnly:
    """Public market data only."""

    def get_prices(self, symbols):
        return {s: PRICE for s in symbols}

    def get_ticker(self, symbol):
        return {"lastPrice": PRICE}


class BrokerTouched(AssertionError):
    pass


class NoBrokerCalls:
    """Any broker call fails the test: the capital verdict must come first."""

    def __getattr__(self, name):
        raise BrokerTouched(name)


def executor(db, *, mode: str, client=None, budget: float = BUDGET, bot: str = BOT):
    ex = BinanceExecutor(
        client=client or MarketOnly(), execution_mode=mode, bot_instance_id=bot, db=db,
    )
    ex._capital_budget = budget
    ex._allocation_type = "fixed_amount"
    ex._allocation_value = 120.0
    return ex


class Ctx:
    bot_instance_id = BOT
    user_id = "user_capital"
    broker_account_id = "brk"
    broker_environment = "demo"
    market_type = "CRYPTO"
    effective_policy_hash = "policy_capital"


class Runner:
    def __init__(self, db, leverage=None):
        self.db = db
        self.run_id = RUN_ID
        self.cycle_id = "cyc"
        self.context = Ctx()
        self.position_manager = None
        self._active_decision_ids = {}
        self._symbol_evidence = {}
        if leverage is not None:
            self._symbol_evidence = {"BTCUSDT": {"sizing": {"leverage": leverage}}}

    def _effective_execution_mode(self):
        return "paper"


def commit(db, *, symbol="BTCUSDT", qty=1.2, price=PRICE, leverage=None, position_id="pos_1"):
    """Open a committed position through the real evidence path."""
    runner = Runner(db, leverage=leverage)
    project_fill(runner, db, {
        "symbol": symbol, "side": "LONG", "action": "OPEN",
        "qty": qty, "price": price, "position_id": position_id,
    })


def committed(db) -> float:
    with db.connect() as conn:
        return float(conn.execute(
            "SELECT COALESCE(SUM(committed_margin),0) FROM positions "
            "WHERE bot_instance_id=? AND status='OPEN'", (BOT,),
        ).fetchone()[0])


# ── Paper ───────────────────────────────────────────────────────────────────


def test_paper_permits_the_first_120_commitment(db):
    ex = executor(db, mode="paper")
    result = ex.execute_signal("BTCUSDT", "BUY", 120.0, leverage_override=1)
    assert result.success, result.error
    assert result.details["capital"]["approved"] is True
    assert result.details["capital"]["approved_margin"] == pytest.approx(120.0)
    assert result.details["leverage"] == 1


def test_paper_rejects_a_second_120_commitment(db):
    commit(db)  # 1.2 BTC at 100, 1x: 120 margin committed
    assert committed(db) == pytest.approx(120.0)
    ex = executor(db, mode="paper")

    result = ex.execute_signal("ETHUSDT", "BUY", 120.0, leverage_override=1)

    assert not result.success
    assert result.status == "INSUFFICIENT_MARGIN"
    assert result.details["reason_code"] == RiskReason.INSUFFICIENT_CAPITAL
    assert ex.paper_executor.get_position("ETHUSDT") is None, "no paper position may open"


def test_paper_shrinks_to_what_remains_never_above_it(db):
    commit(db, qty=1.0)  # 100 committed, 20 left
    ex = executor(db, mode="paper")
    result = ex.execute_signal("ETHUSDT", "BUY", 120.0, leverage_override=1)
    assert result.success
    assert result.details["capital"]["approved_margin"] == pytest.approx(20.0)


# ── Live uses the same single authorisation ─────────────────────────────────


def test_live_rejects_a_second_120_commitment_before_touching_the_broker(db):
    commit(db)
    ex = executor(db, mode="live", client=NoBrokerCalls())
    result = ex.execute_signal("ETHUSDT", "BUY", 120.0, leverage_override=1)
    assert result.status == "INSUFFICIENT_MARGIN"
    assert result.details["reason_code"] == RiskReason.INSUFFICIENT_CAPITAL


@pytest.mark.parametrize("mode", ["paper", "live"])
def test_both_modes_authorise_exactly_once_through_the_same_gate(db, mode, monkeypatch):
    ex = executor(db, mode=mode, client=NoBrokerCalls() if mode == "live" else None)
    calls = []
    original = ex._authorize_capital

    def spy(*args, **kwargs):
        calls.append(args)
        return original(*args, **kwargs)

    monkeypatch.setattr(ex, "_authorize_capital", spy)
    try:
        ex.execute_signal("BTCUSDT", "BUY", 60.0, leverage_override=1)
    except (ExchangeError, BrokerTouched):
        pass  # live continues to the broker after an approval; that is not under test
    assert len(calls) == 1


# ── Fail closed ─────────────────────────────────────────────────────────────


class UnreadableDB:
    def connect(self):
        raise RuntimeError("database gone")


@pytest.mark.parametrize("mode", ["paper", "live"])
def test_an_unreadable_ledger_rejects_the_entry(db, mode):
    ex = executor(db, mode=mode, client=NoBrokerCalls() if mode == "live" else None)
    ex._db = UnreadableDB()
    result = ex.execute_signal("BTCUSDT", "BUY", 60.0, leverage_override=1)
    assert not result.success
    assert result.status == "CAPITAL_LEDGER_UNAVAILABLE"
    assert result.details["reason_code"] == RiskReason.CAPITAL_LEDGER_UNAVAILABLE


def test_a_ledger_exception_is_a_rejection_not_an_authorisation(db, monkeypatch):
    from app.risk import capital_ledger

    def explode(self, **kwargs):
        raise ValueError("chain crashed")

    monkeypatch.setattr(capital_ledger.CapitalLedger, "authorize", explode)
    ex = executor(db, mode="paper")
    result = ex.execute_signal("BTCUSDT", "BUY", 60.0, leverage_override=1)
    assert result.status == "CAPITAL_LEDGER_UNAVAILABLE"
    assert ex.paper_executor.get_position("BTCUSDT") is None


def test_a_managed_bot_without_a_budget_is_rejected(db):
    ex = executor(db, mode="paper", budget=0.0)
    result = ex.execute_signal("BTCUSDT", "BUY", 60.0, leverage_override=1)
    assert not result.success
    assert result.details["reason_code"] == RiskReason.CAPITAL_BUDGET_REQUIRED


def test_an_unmanaged_executor_has_no_bot_budget_to_enforce(db):
    ex = executor(db, mode="paper", budget=0.0, bot="default")
    assert ex._authorize_capital("BTCUSDT", 60.0, 1, 5.0) is None


# ── Committed margin uses the executed leverage ─────────────────────────────


def test_committed_margin_uses_the_executed_leverage_not_one_x(db):
    commit(db, qty=3.6, leverage=3.0)  # 360 notional at 3x
    assert committed(db) == pytest.approx(120.0)


def test_without_leverage_evidence_margin_is_over_reserved_never_under(db):
    commit(db, qty=3.6)  # unknown leverage -> 1x: 360, the safe direction
    assert committed(db) == pytest.approx(360.0)
