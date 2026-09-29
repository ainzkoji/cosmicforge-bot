"""Authenticated DEMO venue validation harness: DEMO-only, withdraw-capable keys never trade, every check is
evidence (never inferred), transfer is reported unavailable on demo, and no secret reaches the evidence."""
from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.models.unified_trading import OrderStatus, Side
from app.venue_validation.demo_validation import (
    FAIL, ORDER_CHECKS, PASS, SKIPPED, UNAVAILABLE_ON_DEMO, ValidationRefused, run_validation,
)
from shared_lib.broker.environment import BrokerEnvironment

SECRET_KEY, SECRET = "demo-key-SHOULD-NOT-LEAK", "demo-secret-SHOULD-NOT-LEAK"


def _auth(broker="bybit", env=BrokerEnvironment.DEMO, base_url=None):
    from shared_lib.broker.environment import resolve_base_url

    return SimpleNamespace(account_id="brk_test", user_id="user-1", broker_type=broker, environment=env,
                           base_url=base_url or resolve_base_url(broker, BrokerEnvironment.DEMO),
                           api_key=SECRET_KEY, api_secret=SECRET, credential_version=2, extra={})


class FakeVenue:
    """A broker with shared state across client instances (a 'restart' sees the same broker truth)."""

    def __init__(self, *, withdraw=False, inspectable=True, lose_position_on_restart=False):
        self.withdraw, self.inspectable, self.lose = withdraw, inspectable, lose_position_on_restart
        self.orders, self.trades, self.position, self.protection, self.clients = {}, [], None, None, 0

    def client(self, auth):
        self.clients += 1
        return FakeClient(self, fresh=self.clients > 1)


class FakeClient:
    def __init__(self, venue, fresh):
        self.v, self.fresh = venue, fresh

    def account(self):
        return {"totalWalletBalance": "5000", "availableBalance": "4900"}

    def account_info(self):
        return {"retCode": 0, "result": {"unifiedMarginStatus": 4}}

    def get_instrument(self, symbol):
        return SimpleNamespace(status="Trading", api_tradable=True, min_qty=0.001, qty_step=0.001, min_notional=5.0,
                               tick_size=0.1)

    def get_account_permissions(self):
        if not self.v.inspectable:
            return {"permissions": {}, "note": "not inspectable"}
        return {"permissions": {"TRADE": True, "WITHDRAW": self.v.withdraw}}

    def get_trading_fee_rates(self, symbol):
        return {"maker": "0.0002", "taker": "0.00055"}

    def get_orderbook(self, symbol, limit=50):
        return {"bids": [[64990.0, 1.0]], "asks": [[65000.0, 1.0]], "time": 1}

    def get_funding(self, symbol):
        return {"fundingRate": "0.0001", "nextFundingTime": "1", "markPrice": "64995"}

    def place_order(self, req):
        oid = f"ord-{len(self.v.orders) + 1}"
        self.v.orders[req.client_order_id] = oid
        self.v.trades.append({"orderId": oid, "qty": str(req.qty), "price": "65000", "commission": "0.01"})
        self.v.position = SimpleNamespace(symbol=req.symbol, quantity=req.qty, side=Side.BUY, entry_price=Decimal("65000"))
        return SimpleNamespace(broker_order_id=oid, status=OrderStatus.FILLED, error_message=None)

    def get_order_by_client_order_id(self, symbol, cid):
        return {"orderId": self.v.orders.get(cid), "status": "Filled"} if cid in self.v.orders else {}

    def user_trades(self, symbol, start_time_ms=None, **kw):
        return list(self.v.trades)

    def get_positions(self):
        if self.fresh and self.v.lose:
            return []
        return [self.v.position] if self.v.position is not None else []

    def place_protection(self, req):
        self.v.protection = (req.sl_price, req.tp_price)
        return SimpleNamespace(status="placed", sl_order_id="sl-1", tp_order_id="tp-1", error=None)

    def cancel_all(self, symbol):
        self.v.protection = None

    def close_position_market(self, symbol):
        self.v.position = None


def test_full_demo_validation_passes_only_on_broker_evidence_and_cleans_up():
    venue = FakeVenue()
    ev = run_validation(auth=_auth(), client_factory=venue.client, symbol="BTCUSDT", submit_orders=True,
                        code_version="abc", sleep=lambda s: None)
    assert ev["verdict"] == "VALIDATED" and all(ev["results"][c] == PASS for c in ORDER_CHECKS)
    assert ev["results"]["P_INTERNAL_TRANSFER"] == UNAVAILABLE_ON_DEMO  # Bybit demo: no wallet API
    assert ev["checks"]["CLEANUP"]["result"] == PASS and venue.position is None and venue.protection is None
    assert ev["checks"]["I_ORDER_SUBMIT"]["evidence"]["client_order_id"].startswith("cfval")
    assert venue.clients == 2  # O: a FRESH client re-read broker truth
    text = json.dumps(ev)
    assert SECRET_KEY not in text and SECRET not in text and "brk_test" not in text


def test_read_only_run_never_places_orders_and_is_not_a_validation():
    venue = FakeVenue()
    ev = run_validation(auth=_auth(), client_factory=venue.client, symbol="BTCUSDT")
    assert venue.orders == {} and all(ev["results"][c] == SKIPPED for c in ORDER_CHECKS)
    assert ev["verdict"] == "INCOMPLETE" and ev["results"]["A_AUTHENTICATION"] == PASS


def test_live_accounts_are_refused():
    with pytest.raises(ValidationRefused):
        run_validation(auth=_auth(env=BrokerEnvironment.LIVE), client_factory=FakeVenue().client, symbol="BTCUSDT",
                       submit_orders=True)


def test_withdraw_capable_key_never_trades():
    venue = FakeVenue(withdraw=True)
    ev = run_validation(auth=_auth(), client_factory=venue.client, symbol="BTCUSDT", submit_orders=True)
    assert ev["results"]["E_PERMISSIONS"] == FAIL and venue.orders == {}
    assert "WITHDRAW" in ev["checks"]["I_ORDER_SUBMIT"]["reason"] and ev["verdict"] == "FAILED"


def test_a_non_demo_host_fails_the_environment_check_and_blocks_orders():
    venue = FakeVenue()
    ev = run_validation(auth=_auth(base_url="https://api.bybit.com"), client_factory=venue.client, symbol="BTCUSDT",
                        submit_orders=True)
    assert ev["results"]["B_ENVIRONMENT"] == FAIL and venue.orders == {}


def test_restart_that_cannot_recover_broker_state_fails():
    venue = FakeVenue(lose_position_on_restart=True)
    ev = run_validation(auth=_auth(), client_factory=venue.client, symbol="BTCUSDT", submit_orders=True,
                        sleep=lambda s: None)
    assert ev["results"]["O_RESTART_RECONCILIATION"] == FAIL and ev["verdict"] == "FAILED"


def test_bingx_uninspectable_permissions_are_skipped_not_passed():
    venue = FakeVenue(inspectable=False)
    ev = run_validation(auth=_auth("bingx"), client_factory=venue.client, symbol="BTC-USDT", submit_orders=True,
                        sleep=lambda s: None)
    assert ev["results"]["E_PERMISSIONS"] == SKIPPED and ev["results"]["P_INTERNAL_TRANSFER"] == UNAVAILABLE_ON_DEMO
    assert ev["verdict"] == "VALIDATED"  # trade permission is then evidenced by the demo order checks themselves
