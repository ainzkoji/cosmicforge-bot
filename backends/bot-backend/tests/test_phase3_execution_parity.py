"""Phase 3: one execution contract for Binance / Bybit / BingX, paginated
dynamic instrument discovery, venue-metadata classification, universe
adapters, capability gating and multi-asset bots."""
from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from shared_lib.broker.capabilities import (
    REASON_EXECUTION_UNVALIDATED_LIVE, Capability, CapabilityState, declared_profile, execution_readiness,
)

from app.exchange.binance.client import BinanceFuturesClient
from app.exchange.bingx.client import BingXClient
from app.exchange.bybit.client import BybitClient
from app.exchange.contract import CANONICAL, EXECUTOR_REQUIRED, contract_report
from app.exchange.instruments import (
    COMMODITIES, CRYPTO, FX, FX_PERPETUAL, TRADFI_PERPETUAL, classify, parse_binance_symbol, parse_bingx_contract,
    parse_bybit_instrument,
)
from app.models.unified_trading import OrderRequest, OrderType, ProtectionRequest, ProtectionUpdateRequest, Side

CLIENTS = {"binance": BinanceFuturesClient("k", "s", base_url="https://x"), "bybit": BybitClient("k", "s", base_url="https://x"),
           "bingx": BingXClient("k", "s", base_url="https://x")}


# ── the contract ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("broker", ["binance", "bybit", "bingx"])
def test_every_client_satisfies_the_executor_and_canonical_contract(broker):
    rep = contract_report(CLIENTS[broker], broker)
    assert rep["executor_missing"] == [] and rep["canonical_missing"] == []
    assert rep["transfer_missing"] == [] and rep["forbidden_present"] == []


def test_executor_contract_covers_what_the_executor_calls():
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "app"
    called = set()
    for rel in ("execution/executor.py", "execution/fill_resolution.py", "execution/position_reconciliation.py"):
        called |= set(re.findall(r"self\.client\.([a-z_]+)\(", (root / rel).read_text()))
    called -= {"get_klines", "get_position_stop", "get_ticker", "_signed_get", "close_position"}  # hasattr-guarded fallbacks
    assert called <= set(EXECUTOR_REQUIRED), sorted(called - set(EXECUTOR_REQUIRED))


def test_capabilities_reflect_implemented_but_unvalidated_parity():
    for b in ("bybit", "bingx"):
        prof = declared_profile(b)
        for c in (Capability.ORDERS, Capability.ORDER_LOOKUP, Capability.FILLS, Capability.PROTECTION_ORDERS,
                  Capability.INSTRUMENT_DISCOVERY):
            assert prof.state(c) == CapabilityState.UNVALIDATED
        assert execution_readiness(b, "demo").permitted
        r = execution_readiness(b, "live")
        assert not r.permitted and r.reason_code == REASON_EXECUTION_UNVALIDATED_LIVE
    assert declared_profile("bingx").state(Capability.TRADFI) == CapabilityState.VENUE_API_UNAVAILABLE
    assert declared_profile("bingx").entry(Capability.FX_PERPETUALS).reason_code == "VENUE_API_NOT_SUPPORTED"
    r = execution_readiness("bingx", "demo", product=Capability.FX_PERPETUALS)
    assert not r.permitted and "fx_perpetuals" in r.missing
    assert execution_readiness("binance", "live").permitted  # no regression


# ── Bybit parity over recorded V5 shapes ───────────────────────────────────

def _bybit(responses):
    c = BybitClient("k", "s", base_url="https://x")
    calls = []

    def req(method, path, payload=None):
        calls.append((method, path, dict(payload or {})))
        r = responses[path]
        return r(payload or {}) if callable(r) else r

    c._request_v5 = req
    c.get_symbol_filters = lambda s: SimpleNamespace(step_size=Decimal("0.001"))
    return c, calls


def test_bybit_order_lifecycle_uses_order_link_id_and_binance_shapes():
    c, calls = _bybit({
        "/v5/position/set-leverage": {"retCode": 0},
        "/v5/order/create": {"retCode": 0, "result": {"orderId": "oid-1", "orderLinkId": "cf-abc"}},
        "/v5/order/realtime": {"retCode": 0, "result": {"list": []}},
        "/v5/order/history": {"retCode": 0, "result": {"list": [{
            "orderId": "oid-1", "orderLinkId": "cf-abc", "orderStatus": "Filled", "cumExecQty": "0.012",
            "avgPrice": "65000", "side": "Buy", "orderType": "Market", "qty": "0.012"}]}},
        "/v5/execution/list": {"retCode": 0, "result": {"list": [{
            "orderId": "oid-1", "execQty": "0.012", "execPrice": "65000", "execFee": "0.39", "feeCurrency": "USDT"}]}},
    })
    order = c.place_order(OrderRequest(symbol="BTCUSDT", side=Side.BUY, type=OrderType.MARKET, qty=Decimal("0.0123"),
                                       leverage=Decimal("5"), client_order_id="cf-abc"))
    create = next(p for m, path, p in calls if path == "/v5/order/create")
    assert create["orderLinkId"] == "cf-abc" and create["qty"] == "0.012" and create["side"] == "Buy"
    assert order.broker_order_id == "oid-1" and order.qty_filled == 0  # async ack: never assumed filled
    view = c.get_order_by_client_order_id("BTCUSDT", "cf-abc")
    assert view["status"] == "FILLED" and view["executedQty"] == "0.012" and view["clientOrderId"] == "cf-abc"
    from app.execution.fill_resolution import resolve_order_fill

    res = resolve_order_fill(c, symbol="BTCUSDT", order_response=order, client_order_id="cf-abc", sleep=lambda s: None)
    assert res.status == "FILLED" and res.executed_qty == pytest.approx(0.012) and res.fees == pytest.approx(0.39)


def test_bybit_protection_is_position_attached_and_amended_in_place():
    c, calls = _bybit({"/v5/position/trading-stop": {"retCode": 0}})
    r = c.place_protection(ProtectionRequest(symbol="BTCUSDT", position_side=Side.BUY, qty=Decimal("1"),
                                             sl_price=Decimal("60000"), tp_price=Decimal("70000")))
    assert r.status == "success" and r.sl_order_id == "POSITION_SL:BTCUSDT"
    u = c.update_protection(ProtectionUpdateRequest(symbol="BTCUSDT", position_side="LONG", new_sl_price=61000, qty=1))
    last = calls[-1][2]
    assert u["status"] == "OK" and last["stopLoss"] == "61000" and "takeProfit" not in last  # TP untouched
    assert not any(path == "/v5/order/cancel" for _, path, _ in calls)  # no naked window


def test_bybit_open_orders_merge_conditional_and_tpsl():
    def realtime(p):
        rows = {"Order": [], "StopOrder": [{"orderId": "c1", "stopOrderType": "Stop", "orderStatus": "Untriggered"}],
                "tpslOrder": [{"orderId": "t1", "stopOrderType": "TakeProfit", "orderStatus": "Untriggered"}]}
        return {"retCode": 0, "result": {"list": rows[p["orderFilter"]]}}

    c, _ = _bybit({"/v5/order/realtime": realtime})
    types = sorted(o["type"] for o in c.open_orders("BTCUSDT"))
    assert types == ["STOP_MARKET", "TAKE_PROFIT_MARKET"]


def test_bybit_discovery_paginates_and_classifies():
    pages = {None: {"list": [{"symbol": "BTCUSDT", "contractType": "LinearPerpetual", "status": "Trading",
                              "baseCoin": "BTC", "quoteCoin": "USDT", "settleCoin": "USDT", "launchTime": "1585526400000",
                              "priceFilter": {"tickSize": "0.10"}, "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001",
                                                                                     "maxOrderQty": "100", "minNotionalValue": "5"},
                              "leverageFilter": {"maxLeverage": "100"}, "fundingInterval": 480}],
                     "nextPageCursor": "p2"},
             "p2": {"list": [{"symbol": "EURUSDT", "contractType": "LinearPerpetual", "status": "Trading",
                              "baseCoin": "EUR", "quoteCoin": "USDT", "settleCoin": "USDT"},
                             {"symbol": "XAUUSDT", "contractType": "LinearPerpetual", "status": "PreLaunch",
                              "baseCoin": "XAU", "quoteCoin": "USDT", "settleCoin": "USDT"}], "nextPageCursor": ""}}
    c, calls = _bybit({"/v5/market/instruments-info": lambda p: {"retCode": 0, "result": pages[p.get("cursor")]}})
    found = {i.venue_symbol: i for i in c.discover_instruments()}
    assert len([x for x in calls if x[1] == "/v5/market/instruments-info"]) == 2
    btc, eur, xau = found["BTCUSDT"], found["EURUSDT"], found["XAUUSDT"]
    assert (btc.asset_class, btc.canonical_symbol, btc.max_leverage, btc.funding_interval_minutes) == (
        CRYPTO, "BTC/USDT:PERP", 100.0, 480)
    assert (eur.asset_class, eur.product_type, eur.canonical_symbol, eur.classification_source) == (
        FX, FX_PERPETUAL, "EUR/USD:FX_PERPETUAL", "SYMBOL_HEURISTIC")
    assert xau.asset_class == COMMODITIES and not xau.api_tradable
    assert eur.to_instrument_key().asset_class == "FX"


# ── BingX parity ───────────────────────────────────────────────────────────

def test_bingx_symbols_round_trip_and_order_identity():
    c = BingXClient("k", "s", base_url="https://x")
    calls = []

    def req(method, path, payload=None):
        calls.append((method, path, dict(payload or {})))
        if path == "/openApi/swap/v2/trade/order" and method == "POST":
            return {"code": 0, "data": {"order": {"orderId": 99, "clientOrderID": "cf-1", "symbol": "ETH-USDT",
                                                  "status": "FILLED", "executedQty": "0.5", "avgPrice": "3000"}}}
        if path == "/openApi/swap/v2/trade/order" and method == "GET":
            return {"code": 0, "data": {"order": {"orderId": 99, "clientOrderId": "cf-1", "symbol": "ETH-USDT",
                                                  "status": "FILLED", "executedQty": "0.5", "avgPrice": "3000"}}}
        if path == "/openApi/swap/v2/trade/leverage":
            return {"code": 0}
        if path == "/openApi/swap/v2/user/positions":
            return {"code": 0, "data": [{"symbol": "ETH-USDT", "positionAmt": "0.5", "positionSide": "LONG",
                                         "avgPrice": "3000"}]}
        raise AssertionError(path)

    c._request = req
    c.get_symbol_filters = lambda s: SimpleNamespace(step_size=Decimal("0.01"))
    o = c.place_order(OrderRequest(symbol="ETHUSDT", side=Side.SELL, type=OrderType.MARKET, qty=Decimal("0.509"),
                                   client_order_id="cf-1"))
    sent = calls[-1][2]
    assert sent["symbol"] == "ETH-USDT" and sent["quantity"] == "0.5" and sent["clientOrderID"] == "cf-1"
    assert o.broker_order_id == "99" and o.qty_filled == Decimal("0.5")
    assert c.get_order_by_client_order_id("ETHUSDT", "cf-1")["symbol"] == "ETHUSDT"
    assert c.position_risk()[0]["symbol"] == "ETHUSDT"  # runtime namespace, never BTC-USDT


def test_bingx_update_protection_places_new_stop_before_cancelling_old():
    c = BingXClient("k", "s", base_url="https://x")
    seq = []
    c.place_stop_market = lambda *a, **k: (seq.append("place_sl"), {"orderId": "new-sl"})[1]
    c.place_take_profit_market = lambda *a, **k: (seq.append("place_tp"), {"orderId": "new-tp"})[1]
    c.cancel_order = lambda s, oid: seq.append(f"cancel:{oid}") or True
    r = c.update_protection(ProtectionUpdateRequest(symbol="ETHUSDT", position_side="LONG", new_sl_price=2900, qty=1,
                                                    old_sl_order_id="old-sl"))
    assert seq == ["place_sl", "cancel:old-sl"] and r["sl_order_id"] == "new-sl"
    c.place_stop_market = lambda *a, **k: {"status": "SKIPPED_NO_POS"}
    with pytest.raises(RuntimeError):
        c.update_protection(ProtectionUpdateRequest(symbol="ETHUSDT", position_side="LONG", new_sl_price=2900, qty=1,
                                                    old_sl_order_id="keep-me"))


def test_bingx_contract_parsing():
    ins = parse_bingx_contract({"symbol": "BTC-USDT", "asset": "BTC", "currency": "USDT", "status": 1,
                                "apiStateOpen": "true", "pricePrecision": 1, "quantityPrecision": 4,
                                "tradeMinQuantity": "0.0001", "tradeMinUSDT": "2"})
    assert ins.api_tradable and ins.tick_size == pytest.approx(0.1) and ins.canonical_symbol == "BTC/USDT:PERP"
    closed = parse_bingx_contract({"symbol": "X-USDT", "status": 1, "apiStateOpen": "false"})
    assert not closed.api_tradable


# ── Binance classification (metadata first) ────────────────────────────────

def test_binance_classification_uses_venue_metadata():
    base = {"status": "TRADING", "quoteAsset": "USDT", "marginAsset": "USDT", "filters": []}
    btc = parse_binance_symbol({**base, "symbol": "BTCUSDT", "baseAsset": "BTC", "contractType": "PERPETUAL",
                                "underlyingType": "COIN"})
    gold = parse_binance_symbol({**base, "symbol": "XAUUSDT", "baseAsset": "XAU", "contractType": "TRADIFI_PERPETUAL",
                                 "underlyingType": "COMMODITY"})
    odd = parse_binance_symbol({**base, "symbol": "ABCUSDT", "baseAsset": "ABC", "contractType": "TRADIFI_PERPETUAL"})
    assert (btc.asset_class, btc.classification_source) == (CRYPTO, "VENUE_METADATA")
    assert gold.asset_class == COMMODITIES and gold.product_type == TRADFI_PERPETUAL
    assert odd.product_type == TRADFI_PERPETUAL and odd.asset_class != CRYPTO  # never guessed into crypto
    assert classify("PEPE", "USDT")[0] == CRYPTO and classify("GBP", "USD")[0] == FX


# ── universe adapters registered for every broker ─────────────────────────

def test_universe_adapters_exist_for_all_three_brokers():
    from app.universe.adapters import adapter_for

    for broker in ("binance", "bybit", "bingx"):
        assert adapter_for(broker, MagicMock()) is not None


# ── 3A catalog + 3E account eligibility ───────────────────────────────────

def _fx(symbol="EURUSDT", tradable=True):
    return parse_bybit_instrument({"symbol": symbol, "contractType": "LinearPerpetual",
                                   "status": "Trading" if tradable else "Closed", "baseCoin": symbol[:3],
                                   "quoteCoin": "USDT", "settleCoin": "USDT"})


def test_catalog_keeps_delisted_rows_and_never_records_partial_discovery(tmp_path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.market_data_schema import ensure_market_data_schema

    from app.exchange.instruments import InstrumentCatalog, sync_instruments

    db = DB(path=str(tmp_path / "cat.db"))
    ensure_market_data_schema(db)
    cat = InstrumentCatalog(db)
    btc = parse_binance_symbol({"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "status": "TRADING",
                                "contractType": "PERPETUAL", "filters": []})
    assert cat.upsert("bybit_linear", "LIVE", [btc, _fx()], 1)["new"] == 2
    assert cat.upsert("bybit_linear", "LIVE", [btc], 2)["delisted"] == 1
    assert [i.venue_symbol for i in cat.list("bybit_linear", "LIVE")] == ["BTCUSDT"]
    with db.connect() as c:
        assert c.execute("SELECT delisted_at_ms FROM venue_instruments WHERE venue_symbol='EURUSDT'").fetchone()[0] == 2
    empty = SimpleNamespace(discover_instruments=lambda: [])
    with pytest.raises(RuntimeError):
        sync_instruments(empty, catalog=cat, venue="bybit_linear", environment="LIVE", now_ms=3)
    assert [i.venue_symbol for i in cat.list("bybit_linear", "LIVE")] == ["BTCUSDT"]  # untouched


def test_account_eligibility_separates_market_availability_from_api_execution():
    from app.exchange.instruments import execution_eligibility

    fx = _fx()
    ok, reasons = execution_eligibility(fx, broker="bybit", environment="demo")
    assert ok, reasons  # Bybit V5 FX perpetual: discovered + adapter usable on DEMO
    ok, reasons = execution_eligibility(fx, broker="bybit", environment="live")
    assert not ok and reasons == ("BROKER_EXECUTION_UNVALIDATED_FOR_LIVE",)
    bingx_fx = parse_bingx_contract({"symbol": "EUR-USDT", "status": 1, "apiStateOpen": "true"})
    ok, reasons = execution_eligibility(bingx_fx, broker="bingx", environment="demo")
    assert not ok and "VENUE_API_NOT_SUPPORTED" in reasons
    ok, reasons = execution_eligibility(_fx(tradable=False), broker="bybit", environment="demo")
    assert "INSTRUMENT_NOT_API_TRADABLE" in reasons
    ok, reasons = execution_eligibility(fx, broker="bybit", environment="demo", permissions={"TRADE": False})
    assert not ok


# ── 3F one bot, several asset classes ─────────────────────────────────────

def test_market_type_parsing_and_validation():
    from app.models.bot_instance_models import CreateBotInstanceRequest
    from app.universe.asset_classes import parse_allowed_asset_classes

    assert parse_allowed_asset_classes("CRYPTO") == ("CRYPTO",)
    assert parse_allowed_asset_classes("FOREX") == ("FX",)
    assert parse_allowed_asset_classes("crypto,fx") == ("CRYPTO", "FX")
    assert parse_allowed_asset_classes("MULTI_ASSET") == ("CRYPTO", "FX")
    with pytest.raises(ValueError):
        parse_allowed_asset_classes("CRYPTO,STONKS")
    import inspect
    src = inspect.getsource(CreateBotInstanceRequest.validate)
    assert "is_valid_market_type" in src


def test_multi_asset_bot_universe_and_per_instrument_classification():
    from app.runner.bot_context import BotRunContext
    from app.trading_intelligence.integration import cycle_shadow as cs
    from app.universe.adapters import underlying_types_for

    ctx = BotRunContext(user_id="u", bot_instance_id="b", broker_account_id="a", symbols=[], strategy_id="s",
                        universe_mode="BROKER", market_type="CRYPTO,FX")
    assert ctx.allowed_asset_classes == ("CRYPTO", "FX")
    assert underlying_types_for(ctx.allowed_asset_classes) == ("COIN", "FX")
    metas = {"BTCUSDT": SimpleNamespace(underlying_type="COIN"), "EURUSDT": SimpleNamespace(underlying_type="FX")}
    runner = SimpleNamespace(context=ctx, _universe_runtime=SimpleNamespace(
        engine=SimpleNamespace(instrument=lambda s: metas.get(s))))
    assert cs._asset_class(runner, "BTCUSDT") == "CRYPTO" and cs._asset_class(runner, "EURUSDT") == "FX"
    assert cs._asset_class(runner) == "CRYPTO"  # bot-level fallback
    single = SimpleNamespace(context=BotRunContext(user_id="u", bot_instance_id="b", broker_account_id="a", symbols=["X"],
                                                   strategy_id="s", market_type="FOREX"), _universe_runtime=None)
    assert cs._asset_class(single, "EURUSD") == "FX"  # unchanged single-class behaviour


def test_fx_closed_session_does_not_stop_crypto_in_a_multi_asset_bot(monkeypatch):
    from app.trading_intelligence.integration import cycle_shadow as cs

    monkeypatch.setattr(cs, "_session_open", lambda runner, now_ms, ac=None: ac != "FX")
    monkeypatch.setattr(cs, "_asset_class", lambda runner, sym=None: "FX" if sym == "EURUSDT" else "CRYPTO")
    monkeypatch.setattr(cs, "_expected_symbols", lambda runner: {"BTCUSDT", "EURUSDT"})
    monkeypatch.setattr(cs, "is_enabled", lambda: True)
    began = {}

    class Coord:
        def begin_bot_cycle(self, **kw):
            began.update(kw)
            return "key"

        def mark_due(self, key, sym):
            began.setdefault("due", set()).add(sym)

    monkeypatch.setattr(cs, "_get_coordinator", lambda: Coord())
    cs.reset_for_tests()
    runner = SimpleNamespace(context=SimpleNamespace(bot_instance_id="bot-ma", user_id="u", broker_account_id="a"),
                             _universe_runtime=object(), run_id="r", interval="15m")
    monkeypatch.setattr(cs, "_bot_id", lambda r: "bot-ma")
    monkeypatch.setattr(cs, "_epoch_id", lambda r, now: 1)
    cs.on_cycle_start(runner)
    assert began["universe_symbols"] == ["BTCUSDT"] and began["due"] == {"BTCUSDT"}
    cs.reset_for_tests()
