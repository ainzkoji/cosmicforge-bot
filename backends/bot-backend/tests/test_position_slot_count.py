"""max_open_positions counts economic positions, not only in-memory state.

Found live on 2026-09-13 right after the per-trade allocation fix was deployed:
the slot count was ``SymbolState`` alone, which misses an entry whose broker
fill quantity was absent from the order response (the runner raises
BROKER_FILL_QUANTITY_UNAVAILABLE before recording it) and every position that
reconciliation adopted from the broker. One cycle opened five 120-USDT entries
against two slots. The old aggregate 120 cap had been hiding this by starving
later entries. Position count is its own gate (MAX_OPEN_POSITIONS) and must see
what actually exists: in-memory state, OPEN ledger rows, in-flight intents.
"""
from __future__ import annotations

import types
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.evidence.fill_bridge import project_fill
from app.evidence.writers import open_bot_run, open_runtime_session
from app.execution.entry_protection import get_entry_protection
from app.runner.runner import PaperRunner
from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import PAPER_FORWARD
from shared_lib.persistence.migrations import migrate

BOT = "bot_slots"
RUN_ID = "run_slots"


@pytest.fixture
def db():
    database = DB(":memory:")
    migrate(database)
    session = open_runtime_session(
        database, database_role="development", database_path=":memory:",
        process_execution_mode="paper", environment_name="slot-tests",
    )
    open_bot_run(
        database, run_id=RUN_ID, bot_instance_id=BOT, runtime_session_id=session,
        user_id="user_slots", policy_hash="policy_slots", provenance=PAPER_FORWARD,
        execution_mode="paper", broker_environment="demo",
    )
    return database


class _Ctx:
    bot_instance_id = BOT
    user_id = "user_slots"
    broker_account_id = "brk_slots"
    broker_environment = "demo"
    market_type = "CRYPTO"
    effective_policy_hash = "policy_slots"


class _FillRunner:
    def __init__(self, db):
        self.db = db
        self.run_id = RUN_ID
        self.cycle_id = "cyc_slots"
        self.context = _Ctx()
        self.position_manager = None
        self._active_decision_ids = {}
        self._symbol_evidence = {}

    def _effective_execution_mode(self):
        return "paper"


def _runner(db, positions=None):
    """The real PaperRunner slot methods, over a minimal runner."""
    state = {
        sym: SimpleNamespace(position=side) for sym, side in (positions or {}).items()
    }
    fake = SimpleNamespace(
        db=db, state=state, context=SimpleNamespace(bot_instance_id=BOT),
        executor=SimpleNamespace(_entry_prot=get_entry_protection(db)),
    )
    fake._ledger_held_symbols = types.MethodType(PaperRunner._ledger_held_symbols, fake)
    fake._economic_open_symbols = types.MethodType(PaperRunner._economic_open_symbols, fake)
    return fake


def _ledger_open(db, symbol, position_id):
    project_fill(_FillRunner(db), db, {
        "symbol": symbol, "side": "SHORT", "action": "OPEN", "qty": 1000.0,
        "price": 0.84, "position_id": position_id, "leverage": 7.0,
    })


def _ledger_close(db, symbol, position_id):
    project_fill(_FillRunner(db), db, {
        "symbol": symbol, "side": "SHORT", "action": "CLOSE", "qty": 1000.0,
        "price": 0.83, "position_id": position_id, "exit_reason": "TEST",
    })


def _in_flight(runner, symbol):
    return runner.executor._entry_prot.acquire_intent(
        bot_id=BOT, symbol=symbol, side="LONG", intended_notional=840.0,
        client_order_id=f"cid_{symbol}", intent_key=f"ik_{symbol}", cycle_id="c1",
        allow_hedge=False,
    )


def test_ledger_and_in_flight_positions_occupy_slots(db):
    runner = _runner(db, {"AAAUSDT": "LONG"})
    _ledger_open(db, "BBBUSDT", "rec_bbb")  # adopted by reconciliation
    assert _in_flight(runner, "CCCUSDT").status.value == "ACQUIRED"
    assert runner._economic_open_symbols() == {"AAAUSDT", "BBBUSDT", "CCCUSDT"}


def test_a_symbol_is_one_slot_whatever_records_it(db):
    runner = _runner(db, {"AAAUSDT": "LONG"})
    _ledger_open(db, "AAAUSDT", "rec_aaa")
    _in_flight(runner, "AAAUSDT")
    assert runner._economic_open_symbols() == {"AAAUSDT"}


def test_closed_positions_and_released_intents_free_their_slots(db):
    runner = _runner(db)
    _ledger_open(db, "BBBUSDT", "rec_bbb")
    _in_flight(runner, "CCCUSDT")
    _ledger_close(db, "BBBUSDT", "rec_bbb")
    runner.executor._entry_prot.mark_failed(BOT, "CCCUSDT", "LONG", reason="test")
    assert runner._economic_open_symbols() == set()


def test_live_regression_positions_unknown_to_memory_block_a_third_entry(db):
    """The 2026-09-13 cycle: nothing in memory, two positions at the broker."""
    from app.policy.policy_engine import PolicyContext, PolicyEngine, ReasonCode

    runner = _runner(db)  # SymbolState knows nothing
    _ledger_open(db, "JCTUSDT", "rec_jct")
    _ledger_open(db, "CHILLGUYUSDT", "rec_chill")

    count = len(runner._economic_open_symbols())
    decision = PolicyEngine().evaluate(PolicyContext(
        symbol="ZZSLOTUSDT", signal="SELL", position="NONE",
        open_positions_count=count, max_open_positions=2, entry_price=1.0,
    ))
    assert count == 2
    assert decision.reason_code == ReasonCode.MAX_POSITIONS_REACHED


def test_both_runner_slot_gates_use_the_economic_count():
    from app.runner import runner as runner_module

    source = Path(runner_module.__file__).read_text(encoding="utf-8")
    assert source.count("len(self._economic_open_symbols())") >= 2
    assert "open_positions_count=sum(" not in source
