"""The order switch stops new exposure; it never strands an open position (gate register section 3.2).

``shared_lib.core.production.risk_reduction_gate``: while an environment's order switch is off, protective stops,
reduce-only closes, cancels and flatten are still admitted -- on DEMO always, on LIVE only in a live-capable reviewed
build. Every build in this repository is not live capable, so LIVE stays incapable of any real-money mutation.
"""
from types import SimpleNamespace

import pytest

from app.core import config
from app.core.config import Settings
from shared_lib.broker.environment import normalize_environment, resolve_base_url
from shared_lib.core import live_build as LB
from shared_lib.core.production import (DemoOrderSubmissionDisabled, LiveOrderSubmissionDisabled,
                                        is_risk_reducing_mutation, order_submission_gate,
                                        require_broker_mutation_permission, risk_reduction_gate)


def profile(demo=False, live=False):
    state = Settings(_env_file=None, APP_ENV="PRODUCTION", DATABASE_ROLE="production",
                     ENVIRONMENT_NAME="production", EXECUTION_MODE="live",
                     DEMO_ORDER_SUBMISSION_ENABLED=demo, LIVE_ORDER_SUBMISSION_ENABLED=False)
    state.LIVE_ORDER_SUBMISSION_ENABLED = live
    return state


@pytest.fixture
def switched_off(monkeypatch):
    monkeypatch.setattr(config, "settings", profile())


def url(broker, env):
    return resolve_base_url(broker, normalize_environment(env))


REDUCING = [
    ("binance", "POST", "/fapi/v1/order", {"symbol": "ADAUSDT", "side": "SELL", "reduceOnly": "true"}),
    ("binance", "POST", "/fapi/v1/algoOrder", {"symbol": "ADAUSDT", "closePosition": "true"}),
    ("binance", "POST", "/fapi/v1/algoOrder", {"symbol": "ADAUSDT", "reduceOnly": "true"}),
    ("binance", "DELETE", "/fapi/v1/order", {"symbol": "ADAUSDT", "orderId": 1}),
    ("binance", "DELETE", "/fapi/v1/algoOrder", {"symbol": "ADAUSDT", "algoId": 1}),
    ("bybit", "POST", "/v5/order/create", {"symbol": "ADAUSDT", "reduceOnly": True}),
    ("bybit", "POST", "/v5/order/cancel", {"symbol": "ADAUSDT", "orderId": "x"}),
    ("bybit", "POST", "/v5/position/trading-stop", {"symbol": "ADAUSDT", "stopLoss": "0.5"}),
    ("bingx", "POST", "/openApi/swap/v2/trade/order", {"symbol": "ADA-USDT", "reduceOnly": "true"}),
    ("bingx", "DELETE", "/openApi/swap/v2/trade/order", {"symbol": "ADA-USDT", "orderId": 1}),
]
INCREASING = [
    ("binance", "POST", "/fapi/v1/order", {"symbol": "ADAUSDT", "side": "BUY"}),
    ("binance", "POST", "/fapi/v1/order", {"symbol": "ADAUSDT", "side": "BUY", "reduceOnly": "false"}),
    ("binance", "POST", "/fapi/v1/leverage", {"symbol": "ADAUSDT", "leverage": 5}),
    ("binance", "POST", "/fapi/v1/marginType", {"symbol": "ADAUSDT"}),
    ("bybit", "POST", "/v5/order/create", {"symbol": "ADAUSDT", "side": "Buy"}),
    ("bybit", "POST", "/v5/position/set-leverage", {"symbol": "ADAUSDT"}),
    ("bingx", "POST", "/openApi/swap/v2/trade/order", {"symbol": "ADA-USDT", "side": "BUY"}),
    ("bingx", "POST", "/openApi/swap/v2/trade/leverage", {"symbol": "ADA-USDT"}),
    ("bingx", "POST", "/openApi/api/v3/post/asset/transfer", {"amount": 1}),
    ("binance", "PUT", "/fapi/v1/listenKey", {}),
]


@pytest.mark.parametrize("broker,method,path,payload", REDUCING)
def test_protection_closes_and_cancels_are_classified_risk_reducing(broker, method, path, payload):
    assert is_risk_reducing_mutation(method, path, broker, payload)


@pytest.mark.parametrize("broker,method,path,payload", INCREASING)
def test_entries_leverage_transfers_and_unknown_paths_are_not(broker, method, path, payload):
    assert not is_risk_reducing_mutation(method, path, broker, payload)


@pytest.mark.parametrize("broker,method,path,payload", REDUCING)
def test_demo_switch_off_admits_only_risk_reducing_mutations(switched_off, broker, method, path, payload):
    assert order_submission_gate("DEMO")["enabled"] is False
    gate = risk_reduction_gate("DEMO")
    assert gate["enabled"] is True and gate["risk_reduction_only"] is True
    assert gate["reason"] == "DEMO_ORDER_SUBMISSION_DISABLED"          # what entries are refused with
    require_broker_mutation_permission(method, path, environment="DEMO", broker=broker,
                                       base_url=url(broker, "demo"), payload=payload)


@pytest.mark.parametrize("broker,method,path,payload", INCREASING)
def test_demo_switch_off_still_refuses_every_risk_increasing_mutation(switched_off, broker, method, path, payload):
    with pytest.raises(DemoOrderSubmissionDisabled):
        require_broker_mutation_permission(method, path, environment="DEMO", broker=broker,
                                           base_url=url(broker, "demo"), payload=payload)


@pytest.mark.parametrize("broker,method,path,payload", REDUCING + INCREASING)
def test_live_stays_incapable_of_any_mutation_in_a_build_that_is_not_live_capable(switched_off, broker, method, path,
                                                                                 payload):
    assert LB.live_build_status().capable is False
    assert risk_reduction_gate("LIVE")["enabled"] is False
    with pytest.raises(LiveOrderSubmissionDisabled):
        require_broker_mutation_permission(method, path, environment="LIVE", broker=broker,
                                           base_url=url(broker, "live"), payload=payload)


def test_a_live_capable_build_with_the_switch_off_protects_but_never_enters(switched_off, monkeypatch):
    monkeypatch.setattr(LB, "live_build_status", lambda: SimpleNamespace(capable=True, reason=LB.CAPABLE))
    gate = risk_reduction_gate("LIVE")
    assert gate["enabled"] is True and gate["risk_reduction_only"] is True
    assert order_submission_gate("LIVE")["enabled"] is False
    base = url("binance", "live")
    require_broker_mutation_permission("POST", "/fapi/v1/algoOrder", environment="LIVE", broker="binance",
                                       base_url=base, payload={"symbol": "ADAUSDT", "closePosition": "true"})
    require_broker_mutation_permission("DELETE", "/fapi/v1/order", environment="LIVE", broker="binance",
                                       base_url=base, payload={"symbol": "ADAUSDT", "orderId": 1})
    with pytest.raises(LiveOrderSubmissionDisabled):
        require_broker_mutation_permission("POST", "/fapi/v1/order", environment="LIVE", broker="binance",
                                           base_url=base, payload={"symbol": "ADAUSDT", "side": "BUY"})


def test_an_undecidable_build_is_not_live_capable(switched_off, monkeypatch):
    def broken():
        raise RuntimeError("unreadable")
    monkeypatch.setattr(LB, "live_build_status", broken)
    assert risk_reduction_gate("LIVE")["enabled"] is False


def test_the_switch_on_is_unchanged(monkeypatch):
    monkeypatch.setattr(config, "settings", profile(demo=True))
    gate = risk_reduction_gate("DEMO")
    assert gate["enabled"] is True and gate["risk_reduction_only"] is False


def test_the_endpoint_check_still_applies_to_risk_reducing_mutations(switched_off):
    with pytest.raises(ValueError, match="BROKER_ENVIRONMENT_MISMATCH"):
        require_broker_mutation_permission("DELETE", "/fapi/v1/order", environment="DEMO", broker="binance",
                                           base_url=url("binance", "live"), payload={"orderId": 1})
