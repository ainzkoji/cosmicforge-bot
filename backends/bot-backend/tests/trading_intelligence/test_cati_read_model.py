"""Step 1.6 -- the CATI account read model shows what the engine knows, from
its own persisted records, with ownership enforced and no exchange call.

The scenarios run the real runtime cycle (``sync_account``) on the
certification broker fake, then read through ``app.core.cati_read_model`` and
the API handlers.
"""
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import HTTPException
from test_demo_boundary_certification import broker
from test_production_execution import live, profile
from test_production_demo_execution import demo, demo_profile
from test_production_research_separation import fresh
from test_runtime_cost_budget import measured, waiting_for_signal
from app.api import cati_account
from app.core import cati_read_model as model
from app.core.bot_instance_service import BotInstanceService
from app.execution import demo_boundary_certification as cert
from app.observability import account_recorder
from app.trading_intelligence.integration import production_runtime as runtime

__all__ = ["broker", "live", "demo", "fresh", "measured"]


def instance_of(m):
    bots = BotInstanceService(db=m.h.db).get_user_bot_instances(m.h.account["user_id"])
    return next(b for b in bots if b.broker_account_id == m.h.account["id"] and b.status == "active")


def protected_cycle(m, monkeypatch):
    waiting_for_signal(monkeypatch)
    cert.run(m.h.db, m.h.account["id"], "read-model", action="hold")
    m.cycle("protected")
    return instance_of(m)


# ── status ──────────────────────────────────────────────────────────────────

def test_status_before_the_first_cycle_is_awaiting_not_running(measured):
    bot = instance_of(measured)
    status = model.bot_status(measured.h.db, bot)
    assert status["engine"]["running"] is False and status["engine"]["last_cycle_at"] is None
    assert status["eligibility"]["reason_code"] == "AWAITING_FIRST_BROKER_SYNC"
    assert status["eligibility"]["eligible_to_enter"] is False and status["eligibility"]["severity"] == "attention"
    assert status["protection_uncertain"] == [] and status["status"] == "deploying"


def test_status_after_a_cycle_reports_the_engine_truth_and_goes_stale(measured, monkeypatch):
    m = measured
    waiting_for_signal(monkeypatch)
    out = m.cycle("waiting")
    bot = instance_of(m)
    status = model.bot_status(m.h.db, bot, now_ms=out["observed_at"] + 1000)
    assert status["engine"]["running"] is True and status["engine"]["stale"] is False
    assert status["eligibility"]["reason_code"] == "AWAITING_NATURAL_CATI_DECISION"
    assert status["eligibility"]["execution_permission"] in ("WAITING_SIGNAL", "BLOCKED_DEMO_ORDER_GATE")
    assert status["eligibility"]["reason"].startswith("No qualifying CATI decision") and status["eligibility"]["suggested_action"]
    assert status["kill_switch"] == {"engaged": False}
    assert status["daily_loss"]["latched"] is False and status["daily_loss"]["equity"] == 5000.0
    assert status["status"] == "running" and status["exchange"] == "BINANCE" and status["environment"] == "DEMO"
    # Three minutes later with no new cycle: never a healthy running engine.
    stale = model.bot_status(m.h.db, bot, now_ms=out["observed_at"] + 180_000)
    assert stale["engine"]["running"] is False and stale["engine"]["stale"] is True
    assert stale["eligibility"]["reason_code"] == "BROKER_SNAPSHOT_STALE" and stale["eligibility"]["eligible_to_enter"] is False


# ── positions ───────────────────────────────────────────────────────────────

def test_positions_show_the_exchange_side_protection_state(measured, monkeypatch):
    m = measured
    bot = protected_cycle(m, monkeypatch)
    [position] = model.positions(m.h.db, bot)
    assert position["symbol"] == "ADAUSDT" and position["side"] == "LONG" and position["quantity"] > 0
    assert position["entry_price"] == pytest.approx(100.0)
    assert position["protection"]["state"] == "CONFIRMED" and position["protection"]["sl_order_id"]
    assert position["stop_price"] and position["target_price"]                 # the plan's own geometry, never invented
    assert position["mark_observed_at"] is not None
    # A failed stop read on the next cycle: UNKNOWN, with the reason, never "confirmed".
    m.h.client.get_algo_orders = lambda *a, **k: (_ for _ in ()).throw(TimeoutError("read timed out"))
    m.cycle("failed_read")
    [position] = model.positions(m.h.db, bot)
    assert position["protection"]["state"] == "UNKNOWN" and position["protection"]["reason"] == "PROTECTION_READ_UNAVAILABLE"
    assert "did not answer" in position["protection"]["description"] or "could not be read" in position["protection"]["description"]
    status = model.bot_status(m.h.db, bot)
    assert [u["symbol"] for u in status["protection_uncertain"]] == ["ADAUSDT"]
    assert status["eligibility"]["reason_code"] == "PROTECTION_STATE_UNKNOWN" and status["eligibility"]["severity"] == "attention"


# ── trades and summary ──────────────────────────────────────────────────────

def test_an_open_trade_is_distinguished_from_a_completed_one(measured, monkeypatch):
    m = measured
    bot = protected_cycle(m, monkeypatch)
    open_trades = model.trades(m.h.db, bot)
    assert open_trades["total"] == 1 and open_trades["trades"][0]["state"] == "OPEN"
    assert open_trades["trades"][0]["net_pnl"] is None                           # open: nothing realized yet
    assert model.trades(m.h.db, bot, include_open=False)["total"] == 0


def test_trades_and_summary_come_from_recorded_fills(measured, monkeypatch):
    m = measured
    waiting_for_signal(monkeypatch)
    cert.run(m.h.db, m.h.account["id"], "read-model", action="hold")
    cert.run(m.h.db, m.h.account["id"], "read-model", action="close")
    m.cycle("closed")
    bot = instance_of(m)
    closed = model.trades(m.h.db, bot, include_open=False)
    assert closed["total"] == 1
    [trade] = closed["trades"]
    assert trade["state"] == "CLOSED" and trade["entry_price"] == pytest.approx(100.0) and trade["exit_price"] == pytest.approx(100.0)
    assert trade["quantity"] == trade["exit_quantity"] > 0 and trade["exit_time"] >= trade["entry_time"]
    assert trade["realized_pnl_gross"] == pytest.approx(0.0) and trade["fees"] == pytest.approx(0.0)
    assert trade["net_pnl"] == pytest.approx(trade["realized_pnl_gross"] - trade["fees"] + trade["funding"])
    assert trade["exit_reason"] in ("STOP_OR_TARGET", "CLOSED") and trade["r_multiple"] == pytest.approx(0.0)
    assert "net_pnl = realized_pnl_gross - fees + funding" in closed["pnl_definition"]
    paged = model.trades(m.h.db, bot, page=2, page_size=1)
    assert paged["trades"] == [] and paged["has_more"] is False
    summary = model.summary(m.h.db, bot)
    for window in ("today", "seven_days", "thirty_days", "all_time"):
        assert summary["windows"][window]["trades"] == 1 and summary["windows"][window]["net_pnl"] == pytest.approx(0.0)
    assert summary["equity"]["current"] == 5000.0 and summary["equity"]["peak_equity"] == 5000.0
    assert summary["equity"]["current_drawdown"] == pytest.approx(0.0)


# ── equity history ──────────────────────────────────────────────────────────

def test_equity_history_is_recorded_hourly_and_on_fills_and_pruned_with_rollups(measured, monkeypatch):
    m = measured
    bot = protected_cycle(m, monkeypatch)
    equity = model.equity(m.h.db, bot)
    reasons = sorted(p["reason"] for p in equity["points"])
    assert reasons == ["FILL", "HOURLY"] and equity["history_available"] and equity["latest"]["equity"] == 5000.0
    assert all(p["source"] == "ENGINE_CYCLE" and p["freshness_ms"] >= 0 for p in equity["points"])
    m.cycle("same_hour")                                                     # the same hour: no second hourly row
    assert len(account_recorder.series(m.h.db, m.h.account["id"])) == 2
    rows = account_recorder.series(m.h.db, m.h.account["id"])
    far_future = rows[-1]["observed_at"] + 100 * 86_400_000
    out = account_recorder.prune(m.h.db, far_future, retention_days=90)
    assert out["deleted"] == 2 and out["rolled_days"] == 1
    assert account_recorder.series(m.h.db, m.h.account["id"]) == []
    [day] = account_recorder.daily(m.h.db, m.h.account["id"])
    assert day["samples"] == 2 and day["high"] == day["low"] == 5000.0
    assert account_recorder.peak_and_drawdown(m.h.db, m.h.account["id"], 4900.0)["peak_equity"] == 5000.0


def test_no_history_is_never_shown_as_zero(measured):
    bot = instance_of(measured)
    equity = model.equity(measured.h.db, bot)
    assert equity["points"] == [] and equity["history_available"] is False and equity["latest"] is None
    assert "nothing is shown as zero" in equity["note"]


# ── isolation and security ──────────────────────────────────────────────────

def test_the_read_model_makes_no_exchange_call(measured, monkeypatch):
    m = measured
    bot = protected_cycle(m, monkeypatch)
    import shared_lib.broker.client_factory as factory
    import shared_lib.broker.resolver as resolver
    monkeypatch.setattr(factory, "build_client_from_auth", Mock(side_effect=AssertionError("exchange call during GET")))
    monkeypatch.setattr(resolver, "resolve_broker_auth", Mock(side_effect=AssertionError("credential read during GET")))
    for method in (m.h.client.account, m.h.client.position_risk, m.h.client.open_orders, m.h.client.get_algo_orders):
        pass
    m.h.client.account = Mock(side_effect=AssertionError("exchange call during GET"))
    m.h.client.position_risk = Mock(side_effect=AssertionError("exchange call during GET"))
    model.bot_status(m.h.db, bot); model.positions(m.h.db, bot); model.trades(m.h.db, bot)
    model.summary(m.h.db, bot); model.equity(m.h.db, bot)


def test_a_recorder_failure_never_interrupts_the_cycle(measured, monkeypatch):
    m = measured
    monkeypatch.setattr(account_recorder, "_insert", Mock(side_effect=RuntimeError("disk full")))
    out = m.cycle("recorder_broken")
    assert out["status"] == "SYNCED"                                          # the cycle completed and was saved
    assert account_recorder.series(m.h.db, m.h.account["id"]) == []


def test_ownership_and_admin_authority_on_the_handlers(measured, monkeypatch):
    m = measured
    bot = protected_cycle(m, monkeypatch)
    service = BotInstanceService(db=m.h.db)
    owner = {"id": bot.user_id}
    other = {"id": "someone-else"}
    status = cati_account.bot_status(bot.id, user=owner, service=service, _perm="x")
    assert status["id"] == bot.id and status["engine"]["running"] is True
    assert cati_account.bot_positions(bot.id, user=owner, service=service, _perm="x")["positions"]
    assert cati_account.bot_trades(bot.id, page=1, page_size=50, include_open=True, user=owner, service=service, _perm="x")["total"] == 1
    assert cati_account.bot_summary(bot.id, user=owner, service=service, _perm="x")["windows"]
    assert cati_account.bot_equity(bot.id, since=None, until=None, limit=100, user=owner, service=service, _perm="x")["points"]
    for handler, extra in ((cati_account.bot_status, {}), (cati_account.bot_positions, {}),
                           (cati_account.bot_trades, dict(page=1, page_size=50, include_open=True)),
                           (cati_account.bot_summary, {}), (cati_account.bot_equity, dict(since=None, until=None, limit=100))):
        with pytest.raises(HTTPException) as denied:
            handler(bot.id, user=other, service=service, _perm="x", **extra)
        assert denied.value.status_code == 404                                # another customer: not revealed
        with pytest.raises(HTTPException) as missing:
            handler("no-such-bot", user=owner, service=service, _perm="x", **extra)
        assert missing.value.status_code == 404
    # The administrator reads any bot, only through the explicit admin routes.
    assert cati_account.admin_bot_status(bot.id, _admin="admin-1", service=service)["id"] == bot.id
    assert cati_account.admin_bot_positions(bot.id, _admin="admin-1", service=service)["positions"]
    payload = json.dumps(cati_account.bot_status(bot.id, user=owner, service=service, _perm="x"), default=str)
    for secret in ("api_key", "api_secret", "mock_ci_key", "hashed_password"):
        assert secret not in payload


def test_a_fill_recorded_outside_the_cycle_still_gets_its_equity_snapshot(measured, monkeypatch):
    """Seen on the connected demo account: the certification close was reconciled
    outside the runtime cycle, so the exit fill never reached the cycle's history
    and got no snapshot. The wallet moving now makes the recorder look it up."""
    import time as _time
    m = measured
    waiting_for_signal(monkeypatch)
    account_recorder._last_wallet.clear()
    m.cycle("first")
    account = m.h.account["id"]
    before = [r for r in account_recorder.series(m.h.db, account) if r["reason"] == "FILL"]
    now = int(_time.time() * 1000)
    with m.h.db.connect() as c:
        c.execute("INSERT INTO cati_production_fills VALUES(?,?,?,?,?)", (account, "old-1", "o-old", "ADAUSDT", json.dumps({"id": "old-1", "time": now - 86_400_000})))
        c.execute("INSERT INTO cati_production_fills VALUES(?,?,?,?,?)", (account, "new-1", "o-new", "ADAUSDT", json.dumps({"id": "new-1", "time": now})))
    m.cycle("wallet_unchanged")                                             # nothing moved: the table is not consulted
    assert [r for r in account_recorder.series(m.h.db, account) if r["reason"] == "FILL"] == before
    real = account_recorder.equity_from_document                              # the exchange now reports a moved wallet
    monkeypatch.setattr(account_recorder, "equity_from_document",
                        lambda document: {**real(document), "equity": 4990.0, "wallet": 4990.0, "available": 4990.0})
    m.cycle("wallet_moved")
    m.cycle("again")
    after = [r for r in account_recorder.series(m.h.db, account) if r["reason"] == "FILL"]
    assert len(after) == len(before) + 1 and after[-1]["equity"] == 4990.0   # the recent fill once; the day-old one never
    with m.h.db.connect() as c:
        keys = [r[0] for r in c.execute("SELECT dedupe_key FROM account_equity_snapshots WHERE reason='FILL'")]
    assert any(k.endswith(":new-1") for k in keys) and not any(k.endswith(":old-1") for k in keys)
