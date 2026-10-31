"""Section K3 -- the read model's trade, summary and status contract.

The fill, plan and attempt sources are replaced by fixed records so the contract itself is under test: every
closed trade counts in the summary (not only the newest page), money is an exact decimal string with its asset,
a cursor walk visits each trade once, and the status lists every reason that blocks a new entry.
"""
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.core import cati_read_model as model

BOT = SimpleNamespace(id="bot-1", broker_account_id="acct-1")
DAY = 86_400_000
NOW = 1_800_000_000_000


def _book(monkeypatch, trades):
    """``trades``: (plan_id, symbol, entry_time, exit_time or None, entry price, exit price, qty, pnl, fee)."""
    fills, plans = [], {}
    for plan_id, symbol, t_in, t_out, p_in, p_out, qty, pnl, fee in trades:
        plans[plan_id] = {"bot_instance_id": BOT.id, "side": "BUY", "stop": str(Decimal(p_in) * Decimal("0.9"))}
        fills.append({"symbol": symbol, "price": p_in, "qty": qty, "time": t_in, "realizedPnl": "0", "commission": fee,
                      "_execution": {"trade_plan_id": plan_id, "leg": "ENTRY"}})
        if t_out is not None:
            fills.append({"symbol": symbol, "price": p_out, "qty": qty, "time": t_out, "realizedPnl": pnl,
                          "commission": fee, "_execution": {"trade_plan_id": plan_id, "leg": "EXIT"}})
    monkeypatch.setattr(model, "_fills", lambda db, account_id: fills)
    monkeypatch.setattr(model, "_plan_index", lambda db, account_id: plans)
    monkeypatch.setattr(model, "_latest_attempts", lambda db, account_id: {})
    monkeypatch.setattr(model, "_funding", lambda *a: Decimal(0))
    monkeypatch.setattr(model, "account_document", lambda db, account_id: None)
    monkeypatch.setattr(model.account_recorder, "peak_and_drawdown",
                        lambda db, account_id, equity: {"peak_equity": None, "current_drawdown": None,
                                                        "current_drawdown_pct": None})


def test_the_summary_counts_every_closed_trade_not_only_the_newest_page(monkeypatch):
    trades = [(f"p{i:04d}", "BTCUSDT", NOW - (300 - i) * DAY, NOW - (300 - i) * DAY + 1, "100", "101", "1", "1", "0.1")
              for i in range(250)]
    _book(monkeypatch, trades)
    out = model.summary(None, BOT, now_ms=NOW)
    all_time = out["windows"]["all_time"]
    assert all_time["trades"] == 250                      # the old code read only trades(page_size=200)
    assert all_time["net_pnl"] == "200" and all_time["fees"] == "50" and all_time["realized_pnl_gross"] == "250"
    assert all_time["wins"] == 250 and out["asset"] == "USDT"


def test_money_is_an_exact_decimal_string_with_its_asset(monkeypatch):
    _book(monkeypatch, [("p1", "ETHUSDT", NOW - 2 * DAY, NOW - DAY, "0.1", "0.3", "3", "0.6", "0.0000001")])
    [trade] = model.trades(None, BOT)["trades"]
    assert trade["asset"] == "USDT"
    assert trade["entry_price"] == "0.1" and trade["exit_price"] == "0.3" and trade["quantity"] == "3"
    # A float sum would give 0.6 - 0.0000002 = 0.5999997999999999.
    assert trade["net_pnl"] == "0.5999998" and trade["fees"] == "0.0000002"
    assert trade["r_multiple"] == "20"                    # 0.6 / ((0.1 - 0.09) * 3)
    window = model.summary(None, BOT, now_ms=NOW)["windows"]["seven_days"]
    assert window["net_pnl"] == "0.5999998" and window["largest_win"] == "0.5999998" and window["largest_loss"] is None


def test_results_in_different_settlement_assets_are_never_added(monkeypatch):
    _book(monkeypatch, [("p1", "BTCUSDT", NOW - 2 * DAY, NOW - DAY, "100", "110", "1", "10", "0"),
                        ("p2", "BTCUSDC", NOW - 2 * DAY, NOW - DAY, "100", "95", "1", "-5", "0")])
    out = model.summary(None, BOT, now_ms=NOW)
    assert out["asset"] is None
    by_asset = out["windows"]["all_time"]["by_asset"]
    assert by_asset["USDT"]["net_pnl"] == "10" and by_asset["USDC"]["net_pnl"] == "-5"


def test_a_cursor_walk_visits_every_trade_once_newest_first(monkeypatch):
    trades = [(f"p{i}", "BTCUSDT", NOW - (50 - i) * DAY, NOW - (50 - i) * DAY + 1, "100", "100", "1", "0", "0")
              for i in range(23)]
    _book(monkeypatch, trades)
    seen, cursor, pages = [], None, 0
    while True:
        page = model.trades(None, BOT, page_size=5, cursor=cursor)
        seen += [t["trade_id"] for t in page["trades"]]
        pages += 1
        cursor = page["next_cursor"]
        if not page["has_more"]:
            assert cursor is None
            break
    assert pages == 5 and seen == [f"p{i}" for i in reversed(range(23))]
    # The classic page parameter still works for existing callers.
    assert [t["trade_id"] for t in model.trades(None, BOT, page=2, page_size=5)["trades"]] == seen[5:10]


def test_an_unreadable_cursor_is_refused(monkeypatch):
    _book(monkeypatch, [])
    with pytest.raises(ValueError, match="INVALID_CURSOR"):
        model.trades(None, BOT, cursor="not-a-cursor")


def test_every_blocking_reason_is_listed_with_its_explanation():
    reasons = model._blocker_reasons(reason="BROKER_SNAPSHOT_STALE", eligibility_reason="FAMILY_STAGE_NO_ORDER_AUTHORITY:M0",
                                     before_gate=None, kill_switch=True, daily_latched=True, uncertain=True)
    codes = [r["code"] for r in reasons]
    assert codes == ["BROKER_SNAPSHOT_STALE", "FAMILY_STAGE_NO_ORDER_AUTHORITY:M0", "CATI_NEW_ENTRY_KILL_SWITCH",
                     "USER_DAILY_LOSS_LIMIT_REACHED", "PROTECTION_STATE_UNKNOWN"]
    assert all(r["message"] and "severity" in r for r in reasons)
    # Nothing blocks: the list is empty (never a placeholder entry).
    assert model._blocker_reasons(reason=None, eligibility_reason=None, before_gate=None, kill_switch=False,
                                  daily_latched=False, uncertain=False) == []
