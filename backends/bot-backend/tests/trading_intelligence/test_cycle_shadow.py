"""Runner-facing shadow wiring: adapters + epoch batching (Sections 14-16)."""
from __future__ import annotations

import time
from types import SimpleNamespace

import pytest
from _helpers import make_evaluated

from app.trading_intelligence.contracts.ranking import SymbolEvalKind, SymbolEvaluation
from app.trading_intelligence.integration import cycle_shadow as cs
from app.trading_intelligence.integration.context_adapters import (
    broker_health_from_sources, event_context_from_records,
)

NOW = 1_700_000_000_000


# -- context adapters ------------------------------------------------------------------
def test_event_feed_stale_or_unknown_is_not_clear():
    assert event_context_from_records([], [], staleness_hours=None, now_ms=NOW).source_state == "UNAVAILABLE"
    assert event_context_from_records([], [], staleness_hours=500.0, now_ms=NOW).source_state == "STALE"
    ctx = event_context_from_records([], [], staleness_hours=1.0, now_ms=NOW, latest_scheduled_ms=NOW + 1)
    assert ctx.source_available is True and ctx.events == ()


def test_event_windows_global_and_scoped():
    active = [
        {"id": 1, "start_utc": "2023-11-14T00:00:00+00:00", "end_utc": "2023-11-15T00:00:00+00:00", "is_global": 1,
         "country_currency": "USD"},
        {"id": 2, "start_utc": "2023-11-14T00:00:00+00:00", "end_utc": "2023-11-15T00:00:00+00:00",
         "is_global": 0, "affected_symbols": "BTCUSDT,ETHUSDT"},
    ]
    ctx = event_context_from_records(active, [{"event_id": "x", "scheduled_utc": "2023-11-14T12:00:00+00:00",
                                               "country_currency": "USD", "event_type": "CPI"}],
                                     staleness_hours=1.0, now_ms=NOW, latest_scheduled_ms=NOW + 1)
    assert len(ctx.events) == 3
    assert {e.event_type for e in ctx.events} >= {"INFLATION"}
    assert any(e.affected_instruments == ("BTCUSDT", "ETHUSDT") for e in ctx.events)


def test_broker_health_unknown_only_when_sources_unreadable():
    kw = dict(broker_account_id="a", venue="v", environment="DEMO", observed_at=NOW)
    assert broker_health_from_sources(circuit_state=None, quarantine_status=None, circuit_readable=False,
                                      quarantine_readable=False, **kw).status == "UNKNOWN"
    assert broker_health_from_sources(circuit_state="NORMAL", quarantine_status="ok", **kw).status == "HEALTHY"
    assert broker_health_from_sources(circuit_state="HALTED", quarantine_status="ok", **kw).status == "UNAVAILABLE"


# -- cycle shadow ----------------------------------------------------------------------
class _Ctx:
    bot_instance_id = "bot1"
    user_id = "u1"
    broker_account_id = None
    max_open_positions = 2


def _runner(symbols=("AAAUSDT", "BBBUSDT")):
    return SimpleNamespace(
        context=_Ctx(), interval="15m", trade_symbols=list(symbols), state={}, _universe_open_symbols=set(),
        _universe_runtime=object(), run_id="r1", cycle_id="c1", db=None, client=None,
    )


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    cs.reset_for_tests()
    yield
    cs.reset_for_tests()


class _FakeController:
    def __init__(self, evals):
        self.evals = evals

    def evaluate_symbol(self, *, snapshot, **kw):
        return self.evals[snapshot]


def _enable(monkeypatch, evals):
    monkeypatch.setenv(cs.ENV_FLAG, "1")
    monkeypatch.setattr(cs, "_get_controller", lambda: _FakeController(evals))
    monkeypatch.setattr(cs, "build_event_risk_context", None, raising=False)
    import app.trading_intelligence.integration.context_adapters as ca

    monkeypatch.setattr(ca, "build_event_risk_context", lambda db, now, **kw: None)
    calls = []
    monkeypatch.setattr(cs, "_portfolio_stage", lambda runner, result: calls.append(result))
    return calls


def _eval(sym, seed):
    ev = make_evaluated(sym, seed=seed)
    return SymbolEvaluation(sym, SymbolEvalKind.EVALUATED.value, opportunities=(ev,))


def test_disabled_by_default_is_noop(monkeypatch):
    monkeypatch.delenv(cs.ENV_FLAG, raising=False)
    r = _runner()
    cs.on_cycle_start(r)
    cs.record_symbol(r, "x", "AAAUSDT", venue="v", source="s")
    cs.on_cycle_end(r, {})
    assert cs._epochs == {} and cs._coordinator is None


def test_hooks_never_raise_on_garbage(monkeypatch):
    monkeypatch.setenv(cs.ENV_FLAG, "1")
    cs.on_cycle_start(object())
    cs.record_symbol(object(), None, "X", venue="v", source="s")
    cs.on_cycle_end(object(), None)


def test_ranks_once_only_when_all_expected_symbols_terminal(monkeypatch):
    evals = {"sa": _eval("AAAUSDT", 1), "sb": _eval("BBBUSDT", 2)}
    calls = _enable(monkeypatch, evals)
    r = _runner()
    cs.on_cycle_start(r)
    cs.record_symbol(r, "sa", "AAAUSDT", venue="v", source="s")
    cs.on_cycle_end(r, {})  # BBB still missing (e.g. deferred by budget)
    assert calls == [] and cs._coordinator.ranking_calls == 0
    cs.on_cycle_start(r)  # next runner cycle, same epoch
    cs.record_symbol(r, "sb", "BBBUSDT", venue="v", source="s")
    cs.on_cycle_end(r, {})
    assert len(calls) == 1 and cs._coordinator.ranking_calls == 1
    assert calls[0].batch.batch_complete is True
    assert cs._epochs == {}


def test_epoch_rollover_with_missing_symbol_fails_closed(monkeypatch):
    evals = {"sa": _eval("AAAUSDT", 1)}
    calls = _enable(monkeypatch, evals)
    r = _runner()
    cs.on_cycle_start(r)
    cs.record_symbol(r, "sa", "AAAUSDT", venue="v", source="s")
    cs.on_cycle_end(r, {})
    cs._epochs["bot1"]["epoch"] -= 900_000  # simulate the candle boundary having passed
    cs.on_cycle_start(r)
    assert calls == []  # incomplete batch: nothing ranked, nothing portfolio-selected
    assert cs._coordinator.ranking_calls == 0


def test_held_symbols_are_not_expected(monkeypatch):
    _enable(monkeypatch, {})
    r = _runner()
    r._universe_open_symbols = {"BBBUSDT"}
    cs.on_cycle_start(r)
    assert cs._epochs["bot1"]["expected"] == {"AAAUSDT"}


def test_non_universe_bot_is_ignored(monkeypatch):
    _enable(monkeypatch, {})
    r = _runner()
    r._universe_runtime = None
    cs.on_cycle_start(r)
    assert cs._epochs == {}


def test_controller_exception_becomes_terminal_failure(monkeypatch):
    calls = _enable(monkeypatch, {})  # KeyError inside evaluate_symbol
    r = _runner(("AAAUSDT",))
    cs.on_cycle_start(r)
    cs.record_symbol(r, "missing", "AAAUSDT", venue="v", source="s")
    cs.on_cycle_end(r, {})
    assert calls == []  # finalized as failed batch -> not ranked
    assert cs._epochs == {}
    assert cs._coordinator.ranking_calls == 0
