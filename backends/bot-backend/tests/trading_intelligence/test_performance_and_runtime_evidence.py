"""Closure items 14 + 15 -- structured CATI_COMPONENT_ERROR evidence (no
secrets) and bounded performance with shared market intelligence."""
from __future__ import annotations

import json
import logging
import time
from types import SimpleNamespace

import pytest
from _pf import add_bot, context, gen, make_db, ranked, select
from conftest import make_binance_klines

from app.runner.market_snapshot import MarketSnapshot
from app.trading_intelligence.contracts.ranking import SymbolEvalKind
from app.trading_intelligence.controller.cati_controller import CATIController
from app.trading_intelligence.integration import cycle_shadow as cs
from app.trading_intelligence.integration.errors import (
    clear_component_errors, recent_component_errors, record_component_error, sanitize_message,
)
from app.trading_intelligence.market_state.engine import get_shared_market_intelligence_service
from app.trading_intelligence.ranking.coordinator import CATICycleCoordinator


# ============================ structured error evidence ============================
def test_sanitizer_strips_credentials():
    msg = ("HTTP 401 apiKey=AKIAabcdef123 signature=deadbeefdeadbeefdeadbeefdeadbeefdeadbeef "
           "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig secret=hunter2 listenKey=abc123")
    clean = sanitize_message(msg)
    for leaked in ("AKIAabcdef123", "deadbeef", "eyJhbGci", "hunter2", "abc123"):
        assert leaked not in clean
    assert len(sanitize_message("x" * 5000)) <= 303


def test_component_error_record_is_structured(caplog):
    clear_component_errors()
    with caplog.at_level(logging.ERROR):
        rec = record_component_error("portfolio.select", RuntimeError("token=SECRET123 boom"), cycle_id="epoch_1",
                                     bot_instance_id="botA", broker_account_id="acct1", symbol="BTCUSDT")
    assert rec.reason_code == "CATI_COMPONENT_ERROR" and rec.exception_class == "RuntimeError"
    assert (rec.component, rec.cycle_id, rec.bot_instance_id, rec.broker_account_id) == ("portfolio.select", "epoch_1", "botA", "acct1")
    assert "SECRET123" not in rec.message
    line = next(r.message for r in caplog.records if "[CATI_COMPONENT_ERROR]" in r.message)
    payload = json.loads(line.split("[CATI_COMPONENT_ERROR] ", 1)[1])
    assert payload["component"] == "portfolio.select" and "SECRET123" not in line
    assert recent_component_errors()[-1] == rec


def test_controller_fault_becomes_terminal_record_with_evidence():
    clear_component_errors()

    class Broken:
        symbol = "BTCUSDT"

        @property
        def candles(self):
            raise ValueError("password=pa55 corrupted snapshot")

    ev = CATIController().evaluate_symbol(snapshot=Broken(), venue="binance", source="t", bot_instance_id="botA",
                                          broker_account_id="acct1", cycle_id="c9")
    assert ev.kind == SymbolEvalKind.CATI_COMPONENT_ERROR.value and "pa55" not in (ev.error or "")
    rec = recent_component_errors()[-1]
    assert rec.component == "controller.evaluate_symbol" and rec.bot_instance_id == "botA" and rec.cycle_id == "c9"


def test_cycle_shadow_failures_leave_evidence_and_never_raise(monkeypatch):
    cs.reset_for_tests()
    clear_component_errors()
    monkeypatch.setenv(cs.ENV_FLAG, "1")

    def explode():
        raise RuntimeError("controller init failed api_secret=zzz")

    monkeypatch.setattr(cs, "_get_controller", explode)
    ctx = SimpleNamespace(bot_instance_id="bot1", user_id="u", broker_account_id="acct9", max_open_positions=1,
                          market_type="CRYPTO", execution_mode="DEMO")
    r = SimpleNamespace(context=ctx, interval="15m", trade_symbols=["AAAUSDT"], state={}, _universe_open_symbols=set(),
                        _universe_runtime=object(), run_id="r", cycle_id="c", db=None, client=None)
    cs.on_cycle_start(r)
    cs.record_symbol(r, "snap", "AAAUSDT", venue="v", source="s")  # must not raise
    rec = recent_component_errors()[-1]
    assert rec.component == "cycle_shadow.record_symbol" and rec.broker_account_id == "acct9"
    assert rec.cycle_id.startswith("epoch_") and "zzz" not in rec.message
    cs.reset_for_tests()


# ================================== performance ==================================
def _snapshots(n):
    return [MarketSnapshot.build(symbol=f"S{i:03d}USDT", timeframe="15m", source="Test",
                                 candles=make_binance_klines(250, seed=i, trend=0.05 * (i % 5 - 2)))
            for i in range(n)]


def test_100_symbol_universe_multi_bot_shares_market_intelligence():
    svc = get_shared_market_intelligence_service()
    svc.clear()
    snaps = _snapshots(100)
    controller = CATIController()
    t = time.perf_counter()
    for bot in ("botA", "botB", "botC"):  # three tenants, same market data
        for s in snaps:
            controller.evaluate_symbol(snapshot=s, venue="binance", source="Test", bot_instance_id=bot,
                                       broker_account_id=f"acct_{bot}")
    elapsed = time.perf_counter() - t
    # identical shared market intelligence is computed ONCE per instrument/candle, not per user
    assert svc.misses == 100 and svc.hits == 200
    assert elapsed < 60.0, elapsed


def test_full_batch_coordination_and_ranking_for_100_symbols_is_bounded():
    snaps = _snapshots(100)
    controller = CATIController()
    c = CATICycleCoordinator()
    key = c.begin_bot_cycle(bot_instance_id="botA", cycle_id="perf", cycle_time_ms=0,
                            universe_symbols=[s.symbol for s in snaps])
    t = time.perf_counter()
    for s in snaps:
        c.mark_due(key, s.symbol)
        c.record_symbol_evaluation(key, controller.evaluate_symbol(snapshot=s, venue="binance", source="Test"))
    res = c.finalize_bot_cycle(key)
    assert res.batch.batch_complete and c.ranking_calls == 1
    assert time.perf_counter() - t < 60.0


def test_portfolio_exact_enumeration_up_to_three_slots_is_bounded():
    cands = [ranked(f"S{i:03d}USDT", 1.0 - i * 0.01, pos=i + 1) for i in range(40)]
    ctx = context(**{f"S{i:03d}USDT": gen(i) for i in range(40)})
    for slots in (1, 2, 3):
        t = time.perf_counter()
        d = select(cands, ctx, slots=slots)
        assert d.solver == "EXACT" and time.perf_counter() - t < 20.0


def test_many_bots_same_account_reservation_throughput(tmp_path):
    from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore
    from _pf import sel, NOW

    db = make_db(tmp_path / "perf.db")
    for i in range(20):
        add_bot(db, f"bot{i}", "acct1")
    store = CATIReservationStore(db)
    t = time.perf_counter()
    outs = [store.reserve(broker_account_id="acct1", bot_instance_id=f"bot{i}", cycle_id="c", selected=[sel(f"T{i:02d}USDT")],
                          now_ms=NOW, ttl_seconds=900) for i in range(20)]
    assert all(o.reserved for o in outs) and time.perf_counter() - t < 20.0
