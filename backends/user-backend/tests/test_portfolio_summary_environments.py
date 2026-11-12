"""Portfolio summary: DEMO and LIVE money are never added together, and no connected account is silently left out.

Before: every connected Binance account's equity was summed into one "Total Portfolio Value" whatever its
environment, Bybit/BingX accounts were omitted from the totals with no error, a missing venue field was read as 0,
and the open positions' unrealized PnL was presented as the day's P&L.
"""
from types import SimpleNamespace

import pytest

from app.core import portfolio_service as PS


ACCOUNT = {"totalMarginBalance": "417.58", "totalWalletBalance": "410.00", "totalUnrealizedProfit": "7.58",
           "totalInitialMargin": "20.00", "availableBalance": "397.58", "totalMaintMargin": "1.00"}


class FakeClient:
    def __init__(self, account, positions=()):
        self._account, self._positions = account, list(positions)

    def account(self):
        if isinstance(self._account, Exception):
            raise self._account
        return self._account

    def position_risk_all(self):
        return self._positions


@pytest.fixture
def world(monkeypatch):
    accounts, envs, clients = [], {}, {}

    def add(account_id, broker, environment, client=None):
        accounts.append({"id": account_id, "broker_id": broker, "status": "connected"})
        envs[account_id] = environment
        clients[account_id] = client

    monkeypatch.setattr(PS, "get_db", lambda: SimpleNamespace(connect=None))
    monkeypatch.setattr(PS, "list_user_broker_accounts", lambda user_id: list(accounts))
    monkeypatch.setattr(PS, "resolve_broker_auth",
                        lambda account_id, user_id, db: SimpleNamespace(environment=envs[account_id],
                                                                        account_id=account_id))
    monkeypatch.setattr(PS, "_build_binance_client", lambda auth: clients[auth.account_id])
    monkeypatch.setattr(PS, "_count_running_bots", lambda db, user_id: 1)
    return add


def summary(world):
    out = PS.get_portfolio_summary("alice")
    return out, out["summary"]


def test_a_demo_only_user_sees_demo_totals_labelled_demo(world):
    world("demo-1", "binance", "demo", FakeClient(ACCOUNT))
    out, s = summary(world)
    assert s["environment"] == "DEMO" and s["mixed_environments"] is False
    assert s["total_portfolio_value"] == 417.58 and s["total_unrealized_pnl"] == 7.58
    assert out["account_errors"] == []


def test_demo_equity_is_never_added_to_live_equity(world):
    world("live-1", "binance", "live", FakeClient({**ACCOUNT, "totalMarginBalance": "1000.00"}))
    world("demo-1", "binance", "testnet", FakeClient(ACCOUNT))
    out, s = summary(world)
    assert s["environment"] == "LIVE" and s["mixed_environments"] is True
    assert s["total_portfolio_value"] == 1000.00                  # not 1,417.58
    assert out["by_environment"]["DEMO"]["equity"] == 417.58
    assert out["by_environment"]["LIVE"]["equity"] == 1000.00


def test_an_account_with_an_unsupported_broker_is_reported_not_silently_omitted(world):
    world("demo-1", "binance", "demo", FakeClient(ACCOUNT))
    world("bybit-1", "bybit", "demo")
    out, s = summary(world)
    assert s["total_portfolio_value"] == 417.58
    assert [(e["account_id"], e["error_type"]) for e in out["account_errors"]] == [
        ("bybit-1", "BROKER_PORTFOLIO_READ_UNSUPPORTED")]


def test_a_missing_venue_field_is_a_failed_read_never_zero(world):
    partial = {k: v for k, v in ACCOUNT.items() if k != "totalMarginBalance"}
    world("demo-1", "binance", "demo", FakeClient(partial))
    out, s = summary(world)
    assert s["environment"] is None and s["total_portfolio_value"] == 0
    assert [(e["account_id"], e["error_type"]) for e in out["account_errors"]] == [("demo-1", "api_error")]


def test_a_failed_read_never_publishes_the_exception_text(world):
    world("demo-1", "binance", "demo",
          FakeClient(RuntimeError("GET https://demo-fapi.binance.com/fapi/v2/account?signature=deadbeef failed")))
    out, _ = summary(world)
    assert out["account_errors"][0]["error"] == "RuntimeError"
    assert "signature" not in repr(out)


def test_positions_carry_their_environment(world):
    world("demo-1", "binance", "demo", FakeClient(ACCOUNT, [
        {"symbol": "ADAUSDT", "positionAmt": "56", "entryPrice": "0.5", "markPrice": "0.51",
         "unRealizedProfit": "0.56", "leverage": "2"}]))
    out, s = summary(world)
    assert [(p["symbol"], p["environment"], p["side"]) for p in out["positions"]] == [("ADAUSDT", "DEMO", "LONG")]
    assert s["open_positions_count"] == 1
