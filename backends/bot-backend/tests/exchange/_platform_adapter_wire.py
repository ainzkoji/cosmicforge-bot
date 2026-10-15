"""Fake HTTP for the three raw venue clients, and the rig the adapter tests run on (test support, no network).

Each ``*Wire`` stands where the raw client's HTTP library stands (``client.session`` for Binance, the module's
``requests`` name for Bybit and BingX), so the REAL raw client runs: its signing, its request building, its
status handling and its parsers. Behind the wire is a ``FakeVenue`` of the matching profile; the wire turns a
venue request into a call on it and renders the outcome in the shape the raw client's parser reads.

What that proves and what it does not. A test on a wire proves that the adapter and the raw client translate
a request into the venue's dialect and an answer of the rendered shape back into the contract. It proves
nothing about what a venue does: the behaviour behind the wire is the fake's.

Where the rendered shapes come from
-----------------------------------
VERIFIED against a recorded venue response (``tests/trading_intelligence/fixtures/binance_demo_recorded.json``,
public endpoints of demo-fapi.binance.com):
    Binance ``exchangeInfo`` symbol record and ``premiumIndex``.

NOT VERIFIED against a venue response; rendered from the keys the raw client's parser (or the repository's
production code) reads:
    Binance  account, positionRisk, openOrders, order, userTrades, leverage, the order answers,
             openAlgoOrders / algoOrder (keys read by ``app.execution.production_protection``), commissionRate
             (keys read by ``app.trading_intelligence.venue.binance``), every error body;
    Bybit    every answer (keys read by ``app.exchange.bybit.client`` and ``app.exchange.instruments``), plus
             ``markPrice``, ``execType``, ``isMaker`` and ``orderLinkId`` which only the adapter reads;
    BingX    every answer (keys read by ``app.exchange.bingx.client`` and ``app.exchange.instruments``); the
             fill row follows ``tests/trading_intelligence/test_perpetual_operational_contract.py``;
    all      which error code a venue returns for which refusal (see ``REFUSALS``): the codes are the ones the
             adapters map, so these tests show the mapping is applied, not that a venue sends that code.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple
from urllib.parse import parse_qsl, urlsplit

import requests

from app.exchange.platform_adapter import contract as c
from app.exchange.platform_adapter.contract import AdapterError, OrderRequest
from app.exchange.platform_adapter.factory import adapter_for
from app.exchange.platform_adapter.fake import FakeVenue
from shared_lib.broker.environment import BrokerEnvironment, resolve_base_url

SECRET_KEY, SECRET = "key-SHOULD-NOT-LEAK-7f3a91", "secret-SHOULD-NOT-LEAK-9c1e55"
BTC, ETH = "BTCUSDT", "ETHUSDT"
RETRY_AFTER_S, BAN_S = 7, 120
#: ids the wire invents for stops a venue stores no client id for; never shown to the adapter
_HIDDEN = "wire:"

#: contract refusal -> the venue code a wire answers with (the reverse of each adapter's table)
REFUSALS: Mapping[str, Mapping[str, int]] = {
    c.BINANCE: {c.INSUFFICIENT_MARGIN: -2019, c.INVALID_QUANTITY: -4023, c.INVALID_PRICE: -4014, c.MIN_NOTIONAL: -4164,
                c.INSTRUMENT_NOT_TRADING: -4140, c.INSTRUMENT_UNKNOWN: -1121, c.ORDER_NOT_FOUND: -2013,
                c.REDUCE_ONLY_REJECTED: -2022, c.STOP_WOULD_TRIGGER_IMMEDIATELY: -2021, c.STOP_ALREADY_EXISTS: -4130,
                c.DUPLICATE_CLIENT_ORDER_ID: -4116, c.LEVERAGE_NOT_ALLOWED: -4028},
    c.BYBIT: {c.INSUFFICIENT_MARGIN: 110007, c.ORDER_NOT_FOUND: 110001, c.REDUCE_ONLY_REJECTED: 110017,
              c.DUPLICATE_CLIENT_ORDER_ID: 110072},
    c.BINGX: {c.INSUFFICIENT_MARGIN: 101204, c.ORDER_NOT_FOUND: 80016, c.INSTRUMENT_NOT_TRADING: 101415,
              c.LEVERAGE_NOT_ALLOWED: 101414},
}
#: a venue code no adapter maps, for every refusal without an entry above
UNMAPPED_CODES = {c.BINANCE: -9999, c.BYBIT: 10001, c.BINGX: 80014}


def _text(value: Optional[float]) -> str:
    return "0" if value is None else repr(float(value)) if float(value) != int(float(value)) else str(int(float(value)))


class Reply:
    """The part of ``requests.Response`` the raw clients read."""

    def __init__(self, status: int = 200, body: Any = None, headers: Optional[Mapping[str, str]] = None,
                 text: Optional[str] = None) -> None:
        self.status_code, self._body, self.headers = status, body, dict(headers or {})
        self.text = text if text is not None else json.dumps(body)
        self.content = self.text.encode("utf-8")

    def json(self) -> Any:
        if self._body is None:
            raise requests.exceptions.JSONDecodeError("Expecting value", self.text, 0)
        return self._body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Error", response=self)


class Wire:
    """Common part: the request log, HTTP-level faults and the bridge to the fake venue."""

    venue = ""
    RequestException = requests.RequestException    # the BingX raw client catches ``requests.RequestException``

    def __init__(self, fake: FakeVenue) -> None:
        self.fake = fake
        self.calls: List[Tuple[str, str, Dict[str, Any]]] = []
        self._faults: List[Dict[str, Any]] = []
        self._scripted: List[Dict[str, Any]] = []
        self._hidden = 0

    # ---------------------------------------------------------------------------------------------- control
    def inject(self, kind: str, *, mutation: bool, times: int = 1) -> None:
        """Make the next ``times`` reads (or mutations) fail at the HTTP level.

        ``rate_limited`` 429 + Retry-After, ``banned`` 418 + Retry-After, ``server_error`` 503, ``timeout_before``
        (no connection), ``timeout_after`` / ``connection_lost`` (the venue acts, the answer is lost),
        ``garbage`` (the venue acts and answers 200 with a body that is not JSON)."""
        self._faults.append({"kind": kind, "mutation": mutation, "remaining": times})

    def answer_next(self, method: str, path: str, reply: Reply) -> None:
        """Answer the next ``method`` request whose path ends with ``path`` with ``reply`` (the fake is not asked)."""
        self._scripted.append({"method": method, "path": path, "reply": reply})

    def sent(self, method: Optional[str] = None, path: str = "") -> List[Tuple[str, str, Dict[str, Any]]]:
        return [call for call in self.calls if method in (None, call[0]) and call[1].endswith(path)]

    def mutations(self) -> List[Tuple[str, str, Dict[str, Any]]]:
        return [call for call in self.calls if call[0] != "GET"]

    # --------------------------------------------------------------------------------------------- serving
    def _serve(self, method: str, url: str, params: Mapping[str, Any], headers: Optional[Mapping[str, Any]]) -> Reply:
        path = urlsplit(url).path
        params = {k: v for k, v in params.items() if k not in ("signature", "timestamp", "recvWindow")}
        self.calls.append((method, path, dict(params)))
        for scripted in self._scripted:
            if scripted["method"] == method and path.endswith(scripted["path"]):
                self._scripted.remove(scripted)
                return scripted["reply"]
        fault = next((f for f in self._faults if f["mutation"] == (method != "GET")), None)
        if fault is None:
            return self._route(method, path, params)
        fault["remaining"] -= 1
        if fault["remaining"] <= 0:
            self._faults.remove(fault)
        kind = fault["kind"]
        # what a transport error text can look like: it names the URL and the headers of the request
        leaky = f"{method} {url} failed; headers={dict(headers or {})}"
        if kind == "timeout_before":
            raise requests.ConnectTimeout(leaky)
        if kind == "rate_limited":
            return Reply(429, self._error_body(-1003, "Too many requests."), {"Retry-After": str(RETRY_AFTER_S)})
        if kind == "banned":
            return Reply(418, self._error_body(-1003, "Banned."), {"Retry-After": str(BAN_S)})
        if kind == "server_error":
            return Reply(503, None, text="<html>Service Unavailable</html>")
        self._route(method, path, params)           # the venue acts; only the answer is lost
        if kind == "timeout_after":
            raise requests.ReadTimeout(leaky)
        if kind == "connection_lost":
            raise requests.ConnectionError(leaky)
        if kind == "garbage":
            return Reply(200, None, text="<html>gateway</html>")
        raise AssertionError(f"unknown fault {kind!r}")

    def _route(self, method: str, path: str, params: Mapping[str, Any]) -> Reply:
        handler = self._routes().get((method, path))
        if handler is None:
            raise AssertionError(f"{self.venue} wire: the adapter sent an unexpected request: {method} {path}")
        try:
            return self._ok(handler(dict(params)))
        except AdapterError as refused:
            return self._refusal(refused)

    def _routes(self) -> Mapping[Tuple[str, str], Callable[[Dict[str, Any]], Any]]:
        raise NotImplementedError

    def _ok(self, body: Any) -> Reply:
        raise NotImplementedError

    def _error_body(self, code: int, message: str) -> Any:
        raise NotImplementedError

    def _refusal(self, refused: AdapterError) -> Reply:
        raise NotImplementedError

    def _code(self, refused: AdapterError) -> int:
        return REFUSALS[self.venue].get(refused.code, UNMAPPED_CODES[self.venue])

    # ------------------------------------------------------------------------------- shared fake accessors
    def _venue_symbol(self, symbol: str) -> str:
        return symbol

    def _symbol(self, venue_symbol: Any) -> str:
        return c.canonical_symbol(venue_symbol)

    def _hidden_id(self) -> str:
        self._hidden += 1
        return f"{_HIDDEN}{self._hidden}"

    @staticmethod
    def _shown(client_order_id: Optional[str]) -> str:
        return "" if not client_order_id or client_order_id.startswith(_HIDDEN) else client_order_id

    def _orders(self, symbol: Optional[str] = None, *, protective: Optional[bool] = None, only_open: bool = True) -> list:
        orders = [o for o in self.fake._orders.values() if symbol in (None, o.symbol)
                  and (protective is None or o.protective == protective) and (o.open or not only_open)]
        return [self.fake._info(o) for o in orders]

    def _lookup(self, symbol: str, *, client_order_id: Optional[str] = None, venue_order_id: Optional[str] = None,
                protective: Optional[bool] = None, only_open: bool = False) -> Any:
        for order in self._orders(symbol, protective=protective, only_open=only_open):
            if (client_order_id and order.client_order_id == client_order_id) or (
                    venue_order_id and order.venue_order_id == str(venue_order_id)):
                return order
        raise AdapterError(c.ORDER_NOT_FOUND, "no such order", venue=self.venue)

    def _refuse_duplicate(self, client_order_id: str) -> None:
        if client_order_id in self.fake._by_client:
            raise AdapterError(c.DUPLICATE_CLIENT_ORDER_ID, "duplicate client order id", venue=self.venue)

    def _position(self, symbol: str) -> Tuple[float, float, int]:
        position = self.fake._positions.get(symbol)
        if position is None:
            return 0.0, 0.0, self.fake.default_leverage
        return float(position.quantity), float(position.entry), position.leverage

    def _unrealized(self, symbol: str) -> float:
        return float(self.fake._unrealized(symbol)) if symbol in self.fake._positions else 0.0


# =================================================================================================== Binance
class BinanceWire(Wire):
    """Stands in for ``BinanceFuturesClient.session`` (a ``requests.Session``)."""

    venue = c.BINANCE

    def request(self, method: str, url: str, params: Any = None, headers: Any = None, timeout: Any = None) -> Reply:
        return self._serve(method.upper(), url, {**dict(parse_qsl(urlsplit(url).query)), **dict(params or {})}, headers)

    def get(self, url: str, params: Any = None, headers: Any = None, timeout: Any = None) -> Reply:
        return self.request("GET", url, params, headers)

    def post(self, url: str, headers: Any = None, timeout: Any = None) -> Reply:
        return self.request("POST", url, None, headers)

    def delete(self, url: str, headers: Any = None, timeout: Any = None) -> Reply:
        return self.request("DELETE", url, None, headers)

    def _ok(self, body: Any) -> Reply:
        return Reply(200, body, {"X-MBX-USED-WEIGHT-1M": str(len(self.calls))})

    def _error_body(self, code: int, message: str) -> Any:
        return {"code": code, "msg": message}

    def _refusal(self, refused: AdapterError) -> Reply:
        return Reply(400, self._error_body(self._code(refused), refused.message or refused.code))

    def _routes(self) -> Mapping[Tuple[str, str], Callable[[Dict[str, Any]], Any]]:
        return {
            ("GET", "/fapi/v1/time"): lambda p: {"serverTime": self.fake.now_ms() + self.fake.server_time_offset_ms},
            ("POST", "/fapi/v1/listenKey"): lambda p: {"listenKey": "synthetic-private-stream-key"},
            ("GET", "/fapi/v1/exchangeInfo"): lambda p: {"serverTime": self.fake.now_ms(), "symbols": [
                self.symbol_record(r) for r in self.fake._rules.values()]},
            ("GET", "/fapi/v1/premiumIndex"): self._premium_index,
            ("GET", "/fapi/v2/account"): self._account,
            ("GET", "/fapi/v2/positionRisk"): self._position_risk,
            ("GET", "/fapi/v1/openOrders"): lambda p: [
                self._order(o) for o in self._orders(p.get("symbol"), protective=False)],
            ("GET", "/fapi/v1/openAlgoOrders"): lambda p: [
                self._algo(o) for o in self._orders(p.get("symbol"), protective=True)],
            ("GET", "/fapi/v1/order"): lambda p: self._order(self._lookup(
                p["symbol"], client_order_id=p.get("origClientOrderId"), venue_order_id=p.get("orderId"), protective=False)),
            ("GET", "/fapi/v1/userTrades"): self._user_trades,
            ("GET", "/fapi/v1/commissionRate"): lambda p: {
                "symbol": p["symbol"], "makerCommissionRate": _text(self.fake.fee_rates(p["symbol"]).maker),
                "takerCommissionRate": _text(self.fake.fee_rates(p["symbol"]).taker)},
            ("POST", "/fapi/v1/leverage"): lambda p: {
                "symbol": p["symbol"], "leverage": self.fake.set_leverage(p["symbol"], int(p["leverage"])),
                "maxNotionalValue": "1000000"},
            ("POST", "/fapi/v1/order"): self._create,
            ("DELETE", "/fapi/v1/order"): lambda p: self._order(self.fake.cancel_order(
                p["symbol"], venue_order_id=self._lookup(p["symbol"], venue_order_id=p["orderId"], protective=False,
                                                         only_open=True).venue_order_id)),
            ("POST", "/fapi/v1/algoOrder"): self._create_algo,
            ("DELETE", "/fapi/v1/algoOrder"): self._cancel_algo,
        }

    def symbol_record(self, rules: Any) -> Dict[str, Any]:
        """The recorded exchangeInfo symbol shape (VERIFIED), filled from the fake's rules."""
        filters: List[Dict[str, Any]] = [
            {"filterType": "PRICE_FILTER", "tickSize": _text(rules.price_tick), "maxPrice": "809484", "minPrice": "0.10"},
            {"filterType": "LOT_SIZE", "stepSize": _text(rules.quantity_step), "minQty": _text(rules.min_quantity),
             "maxQty": _text(rules.max_quantity)},
            {"filterType": "MARKET_LOT_SIZE", "stepSize": _text(rules.quantity_step), "minQty": _text(rules.min_quantity),
             "maxQty": "120"}]
        if rules.min_notional is not None:
            filters.append({"filterType": "MIN_NOTIONAL", "notional": _text(rules.min_notional)})
        return {"symbol": rules.symbol, "pair": rules.symbol, "contractType": "PERPETUAL", "deliveryDate": 4133404802000,
                "status": "TRADING" if rules.status == c.TRADING else "SETTLING", "baseAsset": rules.base_asset,
                "quoteAsset": rules.quote_asset, "marginAsset": rules.margin_asset, "pricePrecision": 2,
                "quantityPrecision": 3, "filters": filters, "orderTypes": ["LIMIT", "MARKET", "STOP_MARKET"],
                "timeInForce": ["GTC", "IOC", "FOK", "GTX"]}

    def _premium_index(self, p: Dict[str, Any]) -> Dict[str, Any]:
        funding = self.fake.funding(p["symbol"])
        return {"symbol": p["symbol"], "markPrice": _text(funding.mark_price), "indexPrice": _text(funding.mark_price),
                "lastFundingRate": _text(funding.rate), "interestRate": "0.00010000",
                "nextFundingTime": funding.next_funding_ms, "time": self.fake.now_ms()}

    def _account(self, p: Dict[str, Any]) -> Dict[str, Any]:
        balance = self.fake.balance()
        return {"totalWalletBalance": _text(balance.wallet_balance), "totalUnrealizedProfit": _text(balance.unrealized_pnl),
                "totalMarginBalance": _text(balance.equity), "availableBalance": _text(balance.available_balance),
                "totalInitialMargin": "0", "assets": [], "positions": []}

    def _position_risk(self, p: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = []
        for symbol in ([p["symbol"]] if p.get("symbol") else list(self.fake._rules)):
            quantity, entry, leverage = self._position(symbol)       # Binance also lists the flat symbols
            rows.append({"symbol": symbol, "positionAmt": _text(quantity), "entryPrice": _text(entry),
                         "markPrice": _text(float(self.fake._prices[symbol])),
                         "unRealizedProfit": _text(self._unrealized(symbol)),
                         "leverage": str(leverage), "marginType": "cross", "positionSide": "BOTH",
                         "updateTime": self.fake.now_ms()})
        return rows

    def _order(self, order: Any) -> Dict[str, Any]:
        return {"symbol": order.symbol, "orderId": int(order.venue_order_id), "clientOrderId": order.client_order_id,
                "status": order.status, "side": order.side, "type": order.order_type, "origQty": _text(order.quantity),
                "executedQty": _text(order.filled_quantity), "avgPrice": _text(order.average_price),
                "stopPrice": _text(order.stop_price), "reduceOnly": order.reduce_only, "closePosition": order.close_position,
                "positionSide": "BOTH", "updateTime": order.updated_at_ms}

    def _algo(self, order: Any) -> Dict[str, Any]:
        return {"algoId": int(order.venue_order_id), "clientAlgoId": order.client_order_id, "algoType": "CONDITIONAL",
                "symbol": order.symbol, "side": order.side, "orderType": order.order_type, "algoStatus": "NEW",
                "triggerPrice": _text(order.stop_price), "closePosition": order.close_position, "reduceOnly": False,
                "updateTime": order.updated_at_ms}

    def _user_trades(self, p: Dict[str, Any]) -> List[Dict[str, Any]]:
        fills = self.fake.fills(p["symbol"], since_ms=int(p["startTime"]) if p.get("startTime") else None)
        return [{"id": int(f.trade_id[1:]), "orderId": int(f.venue_order_id), "symbol": f.symbol, "side": f.side,
                 "qty": _text(f.quantity), "price": _text(f.price), "commission": _text(f.fee),
                 "commissionAsset": f.fee_asset, "realizedPnl": _text(f.realized_pnl or 0.0), "maker": f.is_maker,
                 "buyer": f.side == c.BUY, "time": f.time_ms}
                for f in fills if not p.get("endTime") or f.time_ms <= int(p["endTime"])]

    def _create(self, p: Dict[str, Any]) -> Dict[str, Any]:
        self._refuse_duplicate(p["newClientOrderId"])
        return self._order(self.fake.place_order(OrderRequest(
            symbol=p["symbol"], side=p["side"], order_type=p["type"], client_order_id=p["newClientOrderId"],
            quantity=float(p["quantity"]), price=float(p["price"]) if "price" in p else None,
            reduce_only=p.get("reduceOnly") == "true")))

    def _create_algo(self, p: Dict[str, Any]) -> Dict[str, Any]:
        assert p["algoType"] == "CONDITIONAL" and p["closePosition"] == "true"
        self._refuse_duplicate(p["clientAlgoId"])
        return self._algo(self.fake.place_order(OrderRequest(
            symbol=p["symbol"], side=p["side"], order_type=p["type"], client_order_id=p["clientAlgoId"],
            stop_price=float(p["triggerPrice"]), close_position=True, working_type=p["workingType"])))

    def _cancel_algo(self, p: Dict[str, Any]) -> Dict[str, Any]:
        order = self._lookup(p["symbol"], venue_order_id=p["algoId"], protective=True, only_open=True)
        self.fake.cancel_order(p["symbol"], venue_order_id=order.venue_order_id)
        return {"algoId": int(order.venue_order_id), "clientAlgoId": order.client_order_id, "code": "200", "msg": "success"}


# ===================================================================================================== Bybit
_BYBIT_STATUS = {c.NEW: "New", c.PARTIALLY_FILLED: "PartiallyFilled", c.FILLED: "Filled", c.CANCELED: "Cancelled",
                 c.REJECTED: "Rejected", c.EXPIRED: "Deactivated"}


class BybitWire(Wire):
    """Stands in for the ``requests`` name of ``app.exchange.bybit.client``."""

    venue = c.BYBIT

    def get(self, url: str, headers: Any = None, timeout: Any = None) -> Reply:
        return self._serve("GET", url, dict(parse_qsl(urlsplit(url).query)), headers)

    def post(self, url: str, data: Any = None, headers: Any = None, timeout: Any = None) -> Reply:
        return self._serve("POST", url, json.loads(data or "{}"), headers)

    def _ok(self, body: Any) -> Reply:
        if isinstance(body, Reply):
            return body
        return Reply(200, {"retCode": 0, "retMsg": "OK", "result": body})

    def _error_body(self, code: int, message: str) -> Any:
        return {"retCode": 10006, "retMsg": message}

    def _refusal(self, refused: AdapterError) -> Reply:
        return self.refuse(self._code(refused), refused.message or refused.code)

    @staticmethod
    def refuse(code: int, message: str) -> Reply:
        return Reply(200, {"retCode": code, "retMsg": message, "result": {}})    # Bybit refuses with HTTP 200

    def _routes(self) -> Mapping[Tuple[str, str], Callable[[Dict[str, Any]], Any]]:
        return {
            ("GET", "/v5/market/time"): lambda p: {"timeSecond": str(self.fake.now_ms() // 1000), "timeNano": str(
                (self.fake.now_ms() + self.fake.server_time_offset_ms) * 1_000_000)},
            ("GET", "/v5/market/instruments-info"): self._instruments,
            ("GET", "/v5/account/wallet-balance"): self._wallet,
            ("GET", "/v5/position/list"): self._positions,
            ("GET", "/v5/order/realtime"): lambda p: self._order_list(p, only_open=True),
            ("GET", "/v5/order/history"): lambda p: self._order_list(p, only_open=False),
            ("GET", "/v5/execution/list"): self._executions,
            ("GET", "/v5/account/fee-rate"): lambda p: {"list": [{
                "symbol": p["symbol"], "makerFeeRate": _text(self.fake.fee_rates(p["symbol"]).maker),
                "takerFeeRate": _text(self.fake.fee_rates(p["symbol"]).taker)}]},
            ("GET", "/v5/market/tickers"): self._tickers,
            ("POST", "/v5/position/set-leverage"): self._set_leverage,
            ("POST", "/v5/order/create"): self._create,
            ("POST", "/v5/order/cancel"): self._cancel,
            ("POST", "/v5/position/trading-stop"): self._trading_stop,
        }

    def instrument_record(self, rules: Any) -> Dict[str, Any]:
        return {"symbol": rules.symbol, "contractType": "LinearPerpetual",
                "status": "Trading" if rules.status == c.TRADING else "Closed",
                "baseCoin": rules.base_asset, "quoteCoin": rules.quote_asset, "settleCoin": rules.margin_asset,
                "launchTime": "1585526400000", "priceFilter": {"tickSize": _text(rules.price_tick)},
                "lotSizeFilter": {"qtyStep": _text(rules.quantity_step), "minOrderQty": _text(rules.min_quantity),
                                  "maxOrderQty": _text(rules.max_quantity), "minNotionalValue": _text(rules.min_notional)},
                "leverageFilter": {"maxLeverage": _text(rules.max_leverage)}, "fundingInterval": 480}

    def _instruments(self, p: Dict[str, Any]) -> Dict[str, Any]:
        records = [self.instrument_record(r) for r in self.fake._rules.values()]
        if len(records) < 2:                         # two pages whenever there is something to split
            return {"list": records, "nextPageCursor": ""}
        if p.get("cursor"):
            return {"list": records[1:], "nextPageCursor": ""}
        return {"list": records[:1], "nextPageCursor": "page-2"}

    def _wallet(self, p: Dict[str, Any]) -> Dict[str, Any]:
        if p.get("accountType") != "UNIFIED":
            return {"list": []}
        balance = self.fake.balance()
        return {"list": [{"accountType": "UNIFIED", "totalEquity": _text(balance.equity),
                          "totalWalletBalance": _text(balance.wallet_balance), "totalPerpUPL": _text(balance.unrealized_pnl),
                          "totalAvailableBalance": _text(balance.available_balance), "totalInitialMargin": "0"}]}

    def _position_stop(self, symbol: str, kind: str) -> Any:
        return next((o for o in self._orders(symbol, protective=True) if o.order_type == kind), None)

    def _positions(self, p: Dict[str, Any]) -> Dict[str, Any]:
        rows = []
        for symbol in ([p["symbol"]] if p.get("symbol") else list(self.fake._rules)):
            quantity, entry, leverage = self._position(symbol)
            stop, target = self._position_stop(symbol, c.STOP_MARKET), self._position_stop(symbol, c.TAKE_PROFIT_MARKET)
            rows.append({"symbol": symbol, "side": "Buy" if quantity > 0 else "Sell" if quantity < 0 else "",
                         "size": _text(abs(quantity)), "avgPrice": _text(entry),
                         "markPrice": _text(float(self.fake._prices[symbol])),
                         "unrealisedPnl": _text(self._unrealized(symbol)), "leverage": str(leverage), "tradeMode": 0,
                         "positionIdx": 0, "stopLoss": _text(stop.stop_price) if stop and quantity else "",
                         "takeProfit": _text(target.stop_price) if target and quantity else "",
                         "updatedTime": str(self.fake.now_ms())})
        return {"list": rows, "nextPageCursor": ""}

    def _row(self, order: Any) -> Dict[str, Any]:
        return {"symbol": order.symbol, "orderId": order.venue_order_id, "orderLinkId": self._shown(order.client_order_id),
                "orderStatus": "Untriggered" if order.order_type != c.MARKET and order.stop_price and order.open
                else _BYBIT_STATUS[order.status], "side": "Buy" if order.side == c.BUY else "Sell",
                "orderType": "Market" if order.order_type != c.LIMIT else "Limit", "qty": _text(order.quantity),
                "cumExecQty": _text(order.filled_quantity), "avgPrice": _text(order.average_price),
                "triggerPrice": _text(order.stop_price) if order.stop_price else "",
                "stopOrderType": {c.STOP_MARKET: "StopLoss", c.TAKE_PROFIT_MARKET: "TakeProfit"}.get(order.order_type, ""),
                "reduceOnly": order.reduce_only or order.close_position, "closeOnTrigger": order.close_position,
                "updatedTime": str(order.updated_at_ms)}

    def _order_list(self, p: Dict[str, Any], *, only_open: bool) -> Dict[str, Any]:
        symbol = p.get("symbol")
        if p.get("orderLinkId") or p.get("orderId"):
            rows = [o for o in self._orders(symbol, protective=False, only_open=only_open)
                    if o.client_order_id == p.get("orderLinkId") or o.venue_order_id == p.get("orderId")]
        elif p.get("orderFilter") == "tpslOrder":    # the venue's own rows for the position's stops
            rows = self._orders(symbol, protective=True)
        elif p.get("orderFilter") == "StopOrder":
            rows = []
        else:
            rows = self._orders(symbol, protective=False)
        return {"list": [self._row(o) for o in rows], "nextPageCursor": ""}

    def _executions(self, p: Dict[str, Any]) -> Dict[str, Any]:
        fills = [f for f in self.fake.fills(p["symbol"], since_ms=int(p["startTime"]) if p.get("startTime") else None)
                 if not p.get("endTime") or f.time_ms <= int(p["endTime"])]
        rows = [{"symbol": f.symbol, "orderId": f.venue_order_id, "orderLinkId": self._shown(f.client_order_id),
                 "execId": f.trade_id, "execQty": _text(f.quantity), "execPrice": _text(f.price), "execFee": _text(f.fee),
                 "feeCurrency": f.fee_asset, "execTime": str(f.time_ms), "side": "Buy" if f.side == c.BUY else "Sell",
                 "execType": "Trade", "isMaker": f.is_maker} for f in fills]
        if rows:    # the same list also carries funding settlements: not fills, and the adapter must leave them out
            rows.append({**rows[-1], "execId": "funding-settlement", "execType": "Funding", "orderId": "", "orderLinkId": ""})
        return {"list": rows, "nextPageCursor": ""}

    def _tickers(self, p: Dict[str, Any]) -> Dict[str, Any]:
        funding = self.fake.funding(p["symbol"])
        return {"list": [{"symbol": p["symbol"], "lastPrice": _text(funding.mark_price), "markPrice": _text(funding.mark_price),
                          "fundingRate": _text(funding.rate), "nextFundingTime": str(funding.next_funding_ms)}]}

    def _set_leverage(self, p: Dict[str, Any]) -> Any:
        if self._position(p["symbol"])[2] == int(p["buyLeverage"]) and p["symbol"] in self.fake._positions:
            return self.refuse(110043, "leverage not modified")
        self.fake.set_leverage(p["symbol"], int(p["buyLeverage"]))
        return {}

    def _create(self, p: Dict[str, Any]) -> Dict[str, Any]:
        assert p["category"] == "linear" and p["positionIdx"] == 0
        self._refuse_duplicate(p["orderLinkId"])
        order = self.fake.place_order(OrderRequest(
            symbol=p["symbol"], side=p["side"].upper(), order_type=p["orderType"].upper(), client_order_id=p["orderLinkId"],
            quantity=float(p["qty"]), price=float(p["price"]) if "price" in p else None, reduce_only=bool(p.get("reduceOnly"))))
        return {"orderId": order.venue_order_id, "orderLinkId": order.client_order_id}    # an acknowledgement only

    def _cancel(self, p: Dict[str, Any]) -> Dict[str, Any]:
        order = self._lookup(p["symbol"], venue_order_id=p["orderId"], protective=False, only_open=True)
        self.fake.cancel_order(p["symbol"], venue_order_id=order.venue_order_id)
        return {"orderId": order.venue_order_id, "orderLinkId": order.client_order_id}

    def _trading_stop(self, p: Dict[str, Any]) -> Any:
        symbol = p["symbol"]
        quantity = self._position(symbol)[0]
        if quantity == 0:
            return self.refuse(10001, "can not set tp/sl/ts for zero position")
        assert p["tpslMode"] == "Full" and p["positionIdx"] == 0
        for field, kind in (("stopLoss", c.STOP_MARKET), ("takeProfit", c.TAKE_PROFIT_MARKET)):
            if field not in p:
                continue
            resting = self._position_stop(symbol, kind)
            if float(p[field]) == 0:
                if resting is None:
                    return self.refuse(34040, "not modified")
                self.fake.cancel_order(symbol, venue_order_id=resting.venue_order_id)
            elif resting is None:
                self.fake.place_order(OrderRequest(
                    symbol=symbol, side=c.SELL if quantity > 0 else c.BUY, order_type=kind, client_order_id=self._hidden_id(),
                    stop_price=float(p[field]), close_position=True))
            elif resting.stop_price == float(p[field]):
                return self.refuse(34040, "not modified")
            else:
                self.fake.amend_stop(symbol, client_order_id=resting.client_order_id, new_stop_price=float(p[field]))
        return {}


# ===================================================================================================== BingX
_BINGX_STATUS = {c.NEW: "NEW", c.PARTIALLY_FILLED: "PARTIALLY_FILLED", c.FILLED: "FILLED", c.CANCELED: "CANCELLED",
                 c.REJECTED: "FAILED", c.EXPIRED: "EXPIRED"}


class BingXWire(Wire):
    """Stands in for the ``requests`` name of ``app.exchange.bingx.client``."""

    venue = c.BINGX

    def get(self, url: str, headers: Any = None, timeout: Any = None) -> Reply:
        return self._serve("GET", url, dict(parse_qsl(urlsplit(url).query)), headers)

    def post(self, url: str, data: Any = None, headers: Any = None, timeout: Any = None) -> Reply:
        return self._serve("POST", url, dict(parse_qsl(data or "")), headers)

    def delete(self, url: str, headers: Any = None, timeout: Any = None) -> Reply:
        return self._serve("DELETE", url, dict(parse_qsl(urlsplit(url).query)), headers)

    def _ok(self, body: Any) -> Reply:
        return Reply(200, {"code": 0, "msg": "", "data": body})

    def _error_body(self, code: int, message: str) -> Any:
        return {"code": 100410, "msg": message}

    def _refusal(self, refused: AdapterError) -> Reply:
        return Reply(200, {"code": self._code(refused), "msg": refused.message or refused.code})  # refused with HTTP 200

    def _venue_symbol(self, symbol: str) -> str:
        return self.fake._rules[symbol].venue_symbol if symbol in self.fake._rules else symbol

    def _routes(self) -> Mapping[Tuple[str, str], Callable[[Dict[str, Any]], Any]]:
        return {
            ("GET", "/openApi/swap/v2/server/time"): lambda p: {
                "serverTime": self.fake.now_ms() + self.fake.server_time_offset_ms},
            ("GET", "/openApi/swap/v2/quote/contracts"): lambda p: [self.contract_record(r) for r in self.fake._rules.values()],
            ("GET", "/openApi/swap/v2/user/balance"): self._balance,
            ("GET", "/openApi/swap/v2/user/positions"): self._positions,
            ("GET", "/openApi/swap/v1/positionSide/dual"): lambda p: {"dualSidePosition": "false"},
            ("GET", "/openApi/swap/v2/trade/openOrders"): lambda p: {"orders": [
                self._row(o) for o in self._orders(self._symbol(p["symbol"]) if p.get("symbol") else None)]},
            ("GET", "/openApi/swap/v2/trade/order"): lambda p: {"order": self._row(self._lookup(
                self._symbol(p["symbol"]), client_order_id=p.get("clientOrderId"), venue_order_id=p.get("orderId")))},
            ("GET", "/openApi/swap/v2/trade/allFillOrders"): self._fills,
            ("GET", "/openApi/swap/v2/user/commissionRate"): lambda p: {"commission": {
                "makerCommissionRate": self.fake.maker_fee, "takerCommissionRate": self.fake.taker_fee}},
            ("GET", "/openApi/swap/v2/quote/premiumIndex"): self._premium_index,
            ("POST", "/openApi/swap/v2/trade/leverage"): lambda p: {
                "symbol": p["symbol"], "leverage": self.fake.set_leverage(self._symbol(p["symbol"]), int(p["leverage"]))},
            ("POST", "/openApi/swap/v2/trade/order"): self._create,
            ("DELETE", "/openApi/swap/v2/trade/order"): self._cancel,
        }

    def contract_record(self, rules: Any) -> Dict[str, Any]:
        def precision(step: float) -> int:
            return max(0, -int(format(step, "e").split("e")[1]))

        return {"symbol": rules.venue_symbol, "asset": rules.base_asset, "currency": rules.quote_asset,
                "status": 1 if rules.status == c.TRADING else 25, "apiStateOpen": "true",
                "pricePrecision": precision(rules.price_tick), "quantityPrecision": precision(rules.quantity_step),
                "size": _text(rules.quantity_step), "tradeMinQuantity": _text(rules.min_quantity),
                "tradeMinUSDT": _text(rules.min_notional)}

    def _balance(self, p: Dict[str, Any]) -> Dict[str, Any]:
        balance = self.fake.balance()
        return {"balance": {"asset": "USDT", "balance": _text(balance.wallet_balance), "equity": _text(balance.equity),
                            "unrealisedPNL": _text(balance.unrealized_pnl), "availableMargin": _text(balance.available_balance),
                            "usedMargin": "0", "freezedMargin": "0"}}

    def _positions(self, p: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = []
        for symbol in ([self._symbol(p["symbol"])] if p.get("symbol") else list(self.fake._rules)):
            quantity, entry, leverage = self._position(symbol)
            if quantity:                             # a one-way position, reported under its direction
                rows.append({"symbol": self._venue_symbol(symbol), "positionAmt": _text(abs(quantity)),
                             "positionSide": "LONG" if quantity > 0 else "SHORT", "avgPrice": _text(entry),
                             "unrealisedPNL": _text(self._unrealized(symbol)), "leverage": leverage, "marginMode": "CROSSED"})
        return rows

    def _row(self, order: Any) -> Dict[str, Any]:
        return {"symbol": self._venue_symbol(order.symbol), "orderId": int(order.venue_order_id), "side": order.side,
                "positionSide": "BOTH", "type": order.order_type, "origQty": _text(order.quantity),
                "executedQty": _text(order.filled_quantity), "avgPrice": _text(order.average_price),
                "stopPrice": _text(order.stop_price) if order.stop_price else "", "status": _BINGX_STATUS[order.status],
                "clientOrderId": self._shown(order.client_order_id), "reduceOnly": "true" if order.reduce_only else "false",
                "updateTime": order.updated_at_ms}

    def _fills(self, p: Dict[str, Any]) -> List[Dict[str, Any]]:
        assert p["tradingUnit"] == "COIN"
        fills = self.fake.fills(self._symbol(p["symbol"]), since_ms=int(p["startTs"]))
        return [{"tradeId": f.trade_id, "orderId": int(f.venue_order_id), "symbol": self._venue_symbol(f.symbol),
                 "side": f.side, "qty": _text(f.quantity), "price": _text(f.price), "fee": _text(-f.fee),
                 "realizedPnl": _text(f.realized_pnl or 0.0), "time": f.time_ms}
                for f in fills if f.time_ms <= int(p["endTs"])]

    def _premium_index(self, p: Dict[str, Any]) -> Dict[str, Any]:
        funding = self.fake.funding(self._symbol(p["symbol"]))
        return {"symbol": p["symbol"], "markPrice": _text(funding.mark_price), "indexPrice": _text(funding.mark_price),
                "lastFundingRate": _text(funding.rate), "nextFundingTime": funding.next_funding_ms}

    def _create(self, p: Dict[str, Any]) -> Dict[str, Any]:
        assert p["positionSide"] == "BOTH"
        symbol, trigger = self._symbol(p["symbol"]), p["type"] in (c.STOP_MARKET, c.TAKE_PROFIT_MARKET)
        if trigger:
            assert "clientOrderId" not in p and p["reduceOnly"] == "true"
            request = OrderRequest(symbol=symbol, side=p["side"], order_type=p["type"], client_order_id=self._hidden_id(),
                                   quantity=float(p["quantity"]), stop_price=float(p["stopPrice"]), reduce_only=True,
                                   working_type=p["workingType"])
        else:
            self._refuse_duplicate(p["clientOrderId"])
            request = OrderRequest(symbol=symbol, side=p["side"], order_type=p["type"], client_order_id=p["clientOrderId"],
                                   quantity=float(p["quantity"]), price=float(p["price"]) if "price" in p else None,
                                   reduce_only=p.get("reduceOnly") == "true")
        order = self.fake.place_order(request)
        return {"order": {"symbol": p["symbol"], "orderId": int(order.venue_order_id), "side": order.side,
                          "positionSide": "BOTH", "type": order.order_type,
                          "clientOrderID": self._shown(order.client_order_id)}}       # an acknowledgement only

    def _cancel(self, p: Dict[str, Any]) -> Dict[str, Any]:
        symbol = self._symbol(p["symbol"])
        order = self._lookup(symbol, venue_order_id=p["orderId"], only_open=True)
        return {"order": self._row(self.fake.cancel_order(symbol, venue_order_id=order.venue_order_id))}


# ======================================================================================================= rigs
WIRES = {c.BINANCE: BinanceWire, c.BYBIT: BybitWire, c.BINGX: BingXWire}
REAL_RIGS = tuple(c.VENUES)
FAKE_RIGS = tuple(f"fake-{venue}" for venue in c.VENUES)
ALL_RIGS = REAL_RIGS + FAKE_RIGS


def new_fake(venue: str, environment: str = c.DEMO) -> FakeVenue:
    fake = FakeVenue(venue, environment=environment, default_leverage=5)
    fake.add_instrument(BTC, price=50_000.0, price_tick=0.1, quantity_step=0.001, min_notional=5.0)
    fake.add_instrument(ETH, price=2_500.0, price_tick=0.01, quantity_step=0.01, min_notional=5.0)
    return fake


def raw_client(venue: str, wire: Wire, monkeypatch: Any, environment: str = c.DEMO) -> Any:
    """The REAL raw client class of ``venue`` on the canonical host of ``environment``, talking to ``wire``."""
    base_url = resolve_base_url(venue, BrokerEnvironment(environment.lower()))
    if venue == c.BINANCE:
        from app.exchange.binance.client import BinanceFuturesClient

        # built without __init__, as tests/test_binance_rate_limits.py does: the constructor calls the venue
        client = BinanceFuturesClient.__new__(BinanceFuturesClient)
        client.api_key, client.api_secret, client.base_url = SECRET_KEY, SECRET, base_url
        client.recv_window, client._time_offset_ms, client.last_used_weight_1m = 5000, 0, None
        client._exchange_info_cache, client._exchange_info_cache_ts = None, 0.0
        client.session = wire
        return client
    if venue == c.BYBIT:
        from app.exchange.bybit import client as module

        monkeypatch.setattr(module, "requests", wire)
        return module.BybitClient(SECRET_KEY, SECRET, base_url=base_url)
    from app.exchange.bingx import client as module

    monkeypatch.setattr(module, "requests", wire)
    return module.BingXClient(SECRET_KEY, SECRET, base_url=base_url)


@dataclass
class Rig:
    """One adapter under test, the venue state behind it, and uniform controls for both kinds of adapter."""
    name: str
    venue: str
    adapter: Any
    fake: FakeVenue
    wire: Optional[Wire] = None
    client: Any = None

    @property
    def real(self) -> bool:
        return self.wire is not None

    def maps(self, code: str) -> bool:
        """Does this adapter turn the venue's refusal into ``code``? (The fake raises contract codes itself.)"""
        return not self.real or code in REFUSALS[self.venue]

    def expected(self, code: str) -> str:
        return code if self.maps(code) else c.UNMAPPED_VENUE_ERROR

    def mutations_sent(self) -> int:
        """Mutating requests that reached the venue (wire) or mutating calls made (fake)."""
        return len(self.wire.mutations()) if self.wire is not None else len(self.fake.mutations)

    def effects(self) -> int:
        """State changes of the venue behind the adapter."""
        return self.fake._events

    def fail(self, kind: str, *, method: str, mutation: bool, times: int = 1) -> None:
        """The same fault for a real adapter (at the HTTP level) and for the fake (as the contract error)."""
        if self.wire is not None:
            self.wire.inject(kind, mutation=mutation, times=times)
            return
        now = self.fake.now_ms()
        lost = AdapterError(c.NETWORK_UNCERTAIN if mutation else c.VENUE_UNAVAILABLE)    # a read is never uncertain
        error, after = {
            "rate_limited": (AdapterError(c.RATE_LIMITED, retry_after_s=float(RETRY_AFTER_S), http_status=429), False),
            "banned": (AdapterError(c.BANNED, banned_until_ms=now + BAN_S * 1000, http_status=418), False),
            "timeout_before": (AdapterError(c.VENUE_UNAVAILABLE), False),
            "timeout_after": (lost, mutation),
            "connection_lost": (lost, mutation),
            "garbage": (AdapterError(c.NETWORK_UNCERTAIN if mutation else c.MALFORMED_RESPONSE), mutation),
            "server_error": (lost, False),
        }[kind]
        self.fake.fail_next(method, error, after_effect=after, times=times)

    def stop_request(self, symbol: str, side: str, stop_price: float, client_order_id: str, quantity: float,
                     order_type: str = c.STOP_MARKET) -> OrderRequest:
        """A protective stop in the form this venue has (whole position, or its own quantity)."""
        if self.adapter.capabilities().close_position_stop:
            return OrderRequest(symbol=symbol, side=side, order_type=order_type, client_order_id=client_order_id,
                                stop_price=stop_price, close_position=True)
        return OrderRequest(symbol=symbol, side=side, order_type=order_type, client_order_id=client_order_id,
                            stop_price=stop_price, quantity=quantity, reduce_only=True)

    def unsupported_stop_request(self, symbol: str, side: str, stop_price: float, client_order_id: str,
                                 quantity: float) -> OrderRequest:
        """The stop form this venue does NOT have: part of the position, or the position without a quantity."""
        if self.adapter.capabilities().close_position_stop:
            return OrderRequest(symbol=symbol, side=side, order_type=c.STOP_MARKET, client_order_id=client_order_id,
                                stop_price=stop_price, quantity=quantity / 2, reduce_only=True)
        return OrderRequest(symbol=symbol, side=side, order_type=c.STOP_MARKET, client_order_id=client_order_id,
                            stop_price=stop_price, close_position=True)


def build_rig(name: str, monkeypatch: Any, environment: str = c.DEMO) -> Rig:
    venue = name.replace("fake-", "")
    fake = new_fake(venue, environment)
    if name.startswith("fake-"):
        return Rig(name, venue, fake, fake)
    wire = WIRES[venue](fake)
    client = raw_client(venue, wire, monkeypatch, environment)
    return Rig(name, venue, adapter_for(client, venue, environment, clock=fake.now_ms), fake, wire, client)


def market(symbol: str, side: str, quantity: float, client_order_id: str, *, reduce_only: bool = False) -> OrderRequest:
    return OrderRequest(symbol=symbol, side=side, order_type=c.MARKET, client_order_id=client_order_id,
                        quantity=quantity, reduce_only=reduce_only)


def limit(symbol: str, side: str, quantity: float, price: float, client_order_id: str) -> OrderRequest:
    return OrderRequest(symbol=symbol, side=side, order_type=c.LIMIT, client_order_id=client_order_id,
                        quantity=quantity, price=price)
