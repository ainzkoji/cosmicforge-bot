"""BingX USDT-margined perpetual swap (V2) behind the venue adapter contract (Step 4.1).

A translation layer around ``app.exchange.bingx.client.BingXClient``. The raw client is not changed: it keeps
signing and its symbol form (``BTC-USDT`` at the venue, ``BTCUSDT`` everywhere above it).

Orders, order reads, balance and positions go through the raw client's signed transport (``_request``) and
its order normaliser (``_order_view``) rather than through its convenience methods, because those methods
replace what the venue did not say with a value: a missing balance field becomes ``0.0``, an order state the
client cannot map becomes ``NEW``, a failed cancel becomes ``False``. Every endpoint and request field used
here is one the raw client or ``app.execution.perpetual_protection`` already sends; the exceptions, taken
from the venue's documentation, are the value ``workingType=MARK_PRICE`` (the repository only sends
``CONTRACT_PRICE``) and three answer fields read when present (``asset``, ``unrealizedProfit``,
``markPrice``). Fills, fee rates, funding, leverage and server time use the raw client's own methods.

Protective stops are trigger orders (``STOP_MARKET`` / ``TAKE_PROFIT_MARKET``) with their own quantity and
``reduceOnly``, as the raw client's ``place_stop_market`` sends them. Consequences:

* a stop bound to "the whole position" without a quantity is not carried (``close_position`` is
  ``UNSUPPORTED``): the repository always sends a quantity, and whether the venue accepts none is unknown;
* the repository's own protection code states that BingX trigger orders take no client order id, so none is
  sent. The stop is identified by its venue order id; a request for a stop that already rests with the same
  side, kind, trigger and quantity is that stop;
* whether a second stop may rest beside the first is not established, so ``multiple_stops_per_position`` is
  False and a second stop of the same kind and side is refused with ``STOP_ALREADY_EXISTS`` before anything
  is sent. The raw client's ``update_protection`` assumes the opposite (create, then cancel).

Error codes: none is handled anywhere in this repository. Every entry of ``_CODES`` comes from the venue's
published error list only, and nothing was verified against the venue in this work. See
``docs/exchanges/BINGX_ENVIRONMENT_CERTIFICATION.md`` for what an operator must confirm on a demo account.
"""
from __future__ import annotations

import re
from dataclasses import replace
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import requests

from app.exchange.instruments import parse_bingx_contract

from . import contract as c
from .contract import (AccountBalance, AdapterError, FeeRates, FillInfo, FundingInfo, InstrumentRules, OrderInfo,
                       OrderRequest, PositionInfo)
from .factory import AdapterBase, decimal_text, json_error, number, positive

_ORDER = "/openApi/swap/v2/trade/order"
_HTTP = re.compile(r"BingX HTTP (\d{3}): (.*)", re.DOTALL)
_API_ERROR = re.compile(r"BingX API Error: (.*) \(Code (-?\d+)\)\s*$", re.DOTALL)
_NETWORK_ERROR = "BingX Network Error"
_API_OPEN = (None, True, "true", "True")            # as app.exchange.instruments.parse_bingx_contract reads it
_DUAL_MODES = {True: "HEDGE", "true": "HEDGE", False: "ONE_WAY", "false": "ONE_WAY"}
_MARGIN_MODES = {"ISOLATED": "ISOLATED", "CROSSED": "CROSSED", "CROSS": "CROSSED"}
#: the raw client reads the first spelling; the venue's documentation prints the second
_UNREALIZED_KEYS = ("unrealisedPNL", "unrealizedProfit")


class BingXAdapter(AdapterBase):
    venue = c.BINGX
    _CID_MAX_LENGTH = 40                # repo: bingx/client.py place_order sends clientOrderId.lower()[:40]
    _CID_LOWERCASE = True               # repo: the raw client lower-cases the id on send and on lookup
    _CODES: Mapping[int, str] = {       # all doc (BingX common error codes); none exercised against the venue
        100001: c.AUTH_FAILED,                      # signature verification failed
        100412: c.AUTH_FAILED,                      # null signature
        100413: c.AUTH_FAILED,                      # incorrect API key
        100004: c.PERMISSION_DENIED,                # the key was created without this permission
        100419: c.PERMISSION_DENIED,                # request address is not in the key's allow list
        100421: c.TIMESTAMP_OUT_OF_SYNC,            # null timestamp or timestamp mismatch
        100410: c.RATE_LIMITED,                     # rate limitation
        101204: c.INSUFFICIENT_MARGIN,              # insufficient margin
        80016: c.ORDER_NOT_FOUND,                   # order does not exist
        101415: c.INSTRUMENT_NOT_TRADING,           # the pair is suspended from opening new positions
        101414: c.LEVERAGE_NOT_ALLOWED,             # above the pair's maximum leverage
    }
    _OUTCOME_UNKNOWN = frozenset({100500, 80012, 101500})   # doc: internal error, service unavailable, rpc timeout
    _CAPABILITIES: Mapping[str, bool] = {
        "native_stop_market": True, "native_take_profit": True, "reduce_only": True, "close_position_stop": False,
        "stop_amend_in_place": False, "multiple_stops_per_position": False, "one_way_mode": True, "hedge_mode": False,
        "user_stream": False, "position_stop_endpoint": False, "quantity_stop": True, "stop_client_order_id": False}
    _NOTES: Tuple[str, ...] = (
        "a stop is a trigger order with its own quantity and reduceOnly; close_position is not carried",
        "trigger orders are sent without a client order id: persist venue_order_id",
        "UNVERIFIED: whether a second stop may rest beside the first; treated as not possible",
        "UNVERIFIED: every error-code mapping and every response shape of this venue",
        "client order ids are stored lower-case by the venue; an id with capitals travels as its hash",
        "hedge mode is not carried: orders are sent with positionSide BOTH",
        "a fee of exactly 0 cannot be told from an unreported fee in the raw client's fills and is None",
        "the raw client discards response headers: retry_after_s and the used budget are never known",
        "on the VST demo host balances are virtual (the venue names the asset itself when it reports one)",
        "live private stream is documented; the VST private-stream host remains unverified and refused",
    )

    # ------------------------------------------------------------------------------------ failure surfaces
    def _transport_cause(self, exc: BaseException) -> Optional[BaseException]:
        direct = super()._transport_cause(exc)
        if direct is not None:
            return direct
        # The raw client turns every requests failure into a RuntimeError raised inside the handler, so the
        # original is that error's context. Only this wrapper is followed: any other context is unrelated.
        inner = exc.__context__ if str(exc).startswith(_NETWORK_ERROR) else None
        return inner if isinstance(inner, requests.RequestException) else None

    def _classify(self, exc: BaseException, mutation: bool) -> Optional[AdapterError]:
        text = str(exc)
        answer = _HTTP.search(text)
        if answer:
            code, message = json_error(answer.group(2), "code", "msg")
            return self._fail(int(answer.group(1)), code, message, mutation)
        coded = _API_ERROR.search(text)
        if coded:
            return self._fail(None, int(coded.group(2)), coded.group(1), mutation)
        if text.startswith(_NETWORK_ERROR):
            return self._unanswered(mutation, "network error")
        return None

    def _ask(self, method: str, path: str, payload: Optional[Mapping[str, Any]] = None) -> Mapping[str, Any]:
        """One signed request. The raw transport raises unless the venue answered with code 0."""
        answer = self._call(self._client._request, method, path, dict(payload or {}), mutation=method != "GET")
        return self._mapping(answer, path)

    def _default_venue_symbol(self, symbol: str) -> str:
        return self._client._normalize_symbol(symbol)

    # ------------------------------------------------------------------------------------------ read only
    def instrument_rules(self, symbols: Optional[Sequence[str]] = None) -> Mapping[str, InstrumentRules]:
        records = self._rows(self._ask("GET", "/openApi/swap/v2/quote/contracts").get("data"), "contracts")
        return self._publish((self._contract_rules(r) for r in records if isinstance(r, Mapping)), symbols)

    def _contract_rules(self, record: Mapping[str, Any]) -> Optional[InstrumentRules]:
        base, _, quote = str(record.get("symbol") or "").upper().partition("-")
        state = str(record.get("status"))
        if record.get("apiStateOpen") not in _API_OPEN:
            state += "/apiStateOpen=false"          # listed and trading, but the venue takes no new order by API
        return self._rules(record, self._parsed(parse_bingx_contract, record, venue=self.venue),
                           base=record.get("asset") or base, quote=record.get("currency") or quote, venue_status=state)

    def balance(self) -> AccountBalance:
        data = self._ask("GET", "/openApi/swap/v2/user/balance").get("data")
        wallet = self._mapping(data.get("balance") if isinstance(data, Mapping) else None, "balance")
        unrealized = next((number(wallet.get(k)) for k in _UNREALIZED_KEYS if number(wallet.get(k)) is not None), None)
        if unrealized is None:
            raise self._error(c.MALFORMED_RESPONSE, "balance: the venue did not report unrealised profit")
        return AccountBalance(
            venue=self.venue, margin_asset=str(wallet.get("asset") or "USDT"),  # USDT is the raw client's label
            wallet_balance=self._need(wallet, "balance", "balance"), unrealized_pnl=unrealized,
            equity=self._need(wallet, "equity", "balance"),
            available_balance=self._need(wallet, "availableMargin", "balance"), observed_at_ms=self._now())

    def positions(self, symbol: Optional[str] = None) -> Tuple[PositionInfo, ...]:
        answer = self._ask("GET", "/openApi/swap/v2/user/positions", {"symbol": self._to_venue(symbol)} if symbol else {})
        if "data" not in answer:
            raise self._error(c.MALFORMED_RESPONSE, "positions: the venue's answer carried no data")
        rows = [] if answer["data"] is None else self._rows(answer["data"], "positions")
        out, mode = [], None
        for row in rows:
            row = self._mapping(row, "positions")
            amount, held = self._need(row, "positionAmt", "position"), str(row.get("positionSide") or "").upper()
            if held not in ("BOTH", "LONG", "SHORT"):
                raise self._error(c.MALFORMED_RESPONSE, "position: the venue did not report its side")
            if amount == 0:
                continue
            if held != "BOTH" and mode is None:     # LONG / SHORT rows do not say whether the account is one-way
                mode = self._position_mode() or ""
            out.append(self._position(
                row.get("symbol"), amount if held == "BOTH" else abs(amount) if held == "LONG" else -abs(amount),
                entry=row.get("avgPrice"), mark=row.get("markPrice"),
                unrealized=next((row.get(k) for k in _UNREALIZED_KEYS if row.get(k) is not None), None),
                leverage=row.get("leverage"), margin_mode=_MARGIN_MODES.get(str(row.get("marginMode") or "").upper()),
                position_mode="ONE_WAY" if held == "BOTH" else (mode or None)))
        return tuple(out)

    def _position_mode(self) -> Optional[str]:
        data = self._ask("GET", "/openApi/swap/v1/positionSide/dual").get("data")
        return _DUAL_MODES.get(data.get("dualSidePosition")) if isinstance(data, Mapping) else None

    def open_orders(self, symbol: Optional[str] = None) -> Tuple[OrderInfo, ...]:
        return tuple(self._row_info(row) for row in self._open_rows(self._to_venue(symbol) if symbol else None))

    def order(self, symbol: str, *, client_order_id: Optional[str] = None,
              venue_order_id: Optional[str] = None) -> OrderInfo:
        self._identity(client_order_id, venue_order_id)
        canonical, venue_symbol = c.canonical_symbol(symbol), self._to_venue(symbol)
        target = str(venue_order_id) if venue_order_id else self._bound(client_order_id, canonical)
        identity = {"orderId": target} if target else {"clientOrderId": self._client_id(client_order_id)}
        return self._row_info(self._find(venue_symbol, identity), canonical, client_order_id)

    def fills(self, symbol: str, *, since_ms: Optional[int] = None,
              venue_order_id: Optional[str] = None) -> Tuple[FillInfo, ...]:
        canonical, venue_symbol = c.canonical_symbol(symbol), self._to_venue(symbol)
        found: Dict[str, FillInfo] = {}
        for start, end in self._fill_windows(since_ms):
            for row in self._rows(self._call(self._client.user_trades, venue_symbol, start, end), "allFillOrders"):
                row = self._mapping(row, "allFillOrders")
                fee = positive(row.get("commission"))   # the raw client prints 0 for a fee the venue did not report
                fill = self._fill_info(
                    canonical, trade_id=row.get("id"), venue_order_id=row.get("orderId"), client_order_id=None,
                    side=row.get("side"), quantity=row.get("qty"), price=row.get("price"), fee=fee,
                    fee_asset=row.get("commissionAsset") if fee is not None else None,
                    realized_pnl=number(row.get("realizedPnl")), is_maker=None, time=row.get("time"))
                found[fill.trade_id] = fill
        return tuple(f for f in found.values() if venue_order_id is None or f.venue_order_id == str(venue_order_id))

    def fee_rates(self, symbol: str) -> FeeRates:
        row = self._call(self._client.get_trading_fee_rates, self._to_venue(symbol))
        return FeeRates(venue=self.venue, symbol=c.canonical_symbol(symbol),
                        maker=self._need(row, "maker", "commissionRate"),
                        taker=self._need(row, "taker", "commissionRate"), source="ACCOUNT")

    def funding(self, symbol: str) -> FundingInfo:
        row = self._mapping(self._call(self._client.get_funding, self._to_venue(symbol)), "premiumIndex")
        upcoming, hours = positive(row.get("nextFundingTime")), positive(row.get("fundingIntervalHours"))
        return FundingInfo(venue=self.venue, symbol=c.canonical_symbol(symbol), rate=number(row.get("fundingRate")),
                           next_funding_ms=int(upcoming) if upcoming is not None else None,
                           interval_minutes=int(hours * 60) if hours is not None else None,
                           mark_price=positive(row.get("markPrice")), observed_at_ms=self._now())

    # ------------------------------------------------------------------------------------------ mutations
    def set_leverage(self, symbol: str, leverage: int) -> int:
        # the raw client sets the long and the short side with one request each and keeps neither answer
        self._call(self._client.set_leverage, self._to_venue(symbol), int(leverage), mutation=True)
        return int(leverage)

    def place_order(self, request: OrderRequest) -> OrderInfo:
        venue_symbol = self._to_venue(request.symbol)
        if request.protective:
            return self._place_stop(request, venue_symbol)
        self._entry_only_gtc(request)
        venue_id = self._client_id(request.client_order_id)

        def lookup() -> OrderInfo:
            return self._row_info(self._find(venue_symbol, {"clientOrderId": venue_id}), request.symbol,
                                  request.client_order_id)

        def send() -> OrderInfo:
            payload = {"symbol": venue_symbol, "side": request.side, "positionSide": "BOTH", "type": request.order_type,
                       "quantity": decimal_text(request.quantity), "clientOrderId": venue_id}
            if request.order_type == c.LIMIT:
                payload["price"] = decimal_text(request.price)
            if request.reduce_only:
                payload["reduceOnly"] = "true"
            return self._created(request, venue_symbol, self._ask("POST", _ORDER, payload), resting=False)

        return self._idempotent(request, lookup, send)

    def _place_stop(self, request: OrderRequest, venue_symbol: str) -> OrderInfo:
        if request.close_position:
            raise self._unsupported("a stop without a quantity is not carried for BingX: send reduce_only and a quantity")
        wanted = (decimal_text(request.stop_price), decimal_text(request.quantity))
        for row in self._open_rows(venue_symbol):
            view = self._call(self._client._order_view, row)
            if (view.get("type"), view.get("side")) != (request.order_type, request.side):
                continue
            trigger, quantity = positive(view.get("stopPrice")), positive(view.get("origQty"))
            same = trigger is not None and quantity is not None and (decimal_text(trigger), decimal_text(quantity)) == wanted
            if not (same and (view.get("reduceOnly") or view.get("closePosition"))):
                raise self._error(c.STOP_ALREADY_EXISTS, "another stop of this kind and side already rests: cancel it first")
            # the venue stores no client id on a trigger order: the same geometry is the same stop
            found = self._row_info(row, request.symbol, request.client_order_id)
            self._bind(request.client_order_id, request.symbol, str(found.venue_order_id))
            return found
        payload = {"symbol": venue_symbol, "side": request.side, "positionSide": "BOTH", "type": request.order_type,
                   "quantity": wanted[1], "stopPrice": wanted[0], "reduceOnly": "true", "workingType": request.working_type}
        placed = self._created(request, venue_symbol, self._ask("POST", _ORDER, payload), resting=True)
        self._bind(request.client_order_id, request.symbol, str(placed.venue_order_id))
        return placed

    def cancel_order(self, symbol: str, *, client_order_id: Optional[str] = None,
                     venue_order_id: Optional[str] = None) -> OrderInfo:
        found = self.order(symbol, client_order_id=client_order_id, venue_order_id=venue_order_id)
        if found.final:
            return found
        venue_symbol, target = self._to_venue(symbol), str(found.venue_order_id or "")
        if not target:
            raise self._error(c.MALFORMED_RESPONSE, "the venue returned an order without an id; it cannot be cancelled")
        answer = self._ask("DELETE", _ORDER, {"symbol": venue_symbol, "orderId": target})
        fallback = replace(found, status=c.UNKNOWN, venue_status="CANCEL_ACKNOWLEDGED", updated_at_ms=self._now())
        stated = self._stated(answer)
        if stated is not None and stated.get("status"):
            return self._read_back(lambda: self._row_info({**stated, "symbol": stated.get("symbol") or venue_symbol},
                                                          found.symbol, found.client_order_id), fallback)
        return self._read_back(lambda: self._row_info(self._find(venue_symbol, {"orderId": target}), found.symbol,
                                                      found.client_order_id), fallback)

    # ------------------------------------------------------------------------------------------- internals
    @staticmethod
    def _stated(answer: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
        """The order record inside a create or cancel answer (``data.order``, or ``data`` itself)."""
        data = answer.get("data")
        order = (data.get("order") or data) if isinstance(data, Mapping) else None
        return order if isinstance(order, Mapping) else None

    def _created(self, request: OrderRequest, venue_symbol: str, answer: Mapping[str, Any], *, resting: bool) -> OrderInfo:
        order = self._stated(answer)
        if order is None or order.get("orderId") in (None, ""):
            raise self._unconfirmed("order id")
        order_id = str(order["orderId"])
        if resting:     # an accepted trigger order rests; the caller reads protection back
            fallback = self._acknowledged(request, order_id, status=c.NEW, venue_status="ACCEPTED")
        else:
            fallback = self._acknowledged(request, order_id, status=c.UNKNOWN, venue_status="ACKNOWLEDGED")
        if order.get("status"):
            return self._read_back(lambda: self._row_info({**order, "symbol": order.get("symbol") or venue_symbol},
                                                          request.symbol, request.client_order_id), fallback)
        if resting:
            return fallback
        return self._read_back(lambda: self._row_info(self._find(venue_symbol, {"orderId": order_id}), request.symbol,
                                                      request.client_order_id), fallback)

    def _find(self, venue_symbol: str, identity: Mapping[str, str]) -> Mapping[str, Any]:
        data = self._ask("GET", _ORDER, {"symbol": venue_symbol, **identity}).get("data")
        order = data.get("order") if isinstance(data, Mapping) else None
        if not isinstance(order, Mapping) or not order:
            raise self._error(c.ORDER_NOT_FOUND, "the venue holds no such order")
        return order

    def _open_rows(self, venue_symbol: Optional[str]) -> List[Mapping[str, Any]]:
        scope = {"symbol": venue_symbol} if venue_symbol else {}
        data = self._ask("GET", "/openApi/swap/v2/trade/openOrders", scope).get("data")
        if not isinstance(data, Mapping) or "orders" not in data:
            # an answer without the list is not an empty list: it must never read as "no stop rests"
            raise self._error(c.MALFORMED_RESPONSE, "openOrders: the venue's answer carried no order list")
        if data["orders"] is None:
            return []
        return [self._mapping(o, "openOrders") for o in self._rows(data["orders"], "openOrders")]

    def _row_info(self, row: Mapping[str, Any], symbol: Optional[str] = None,
                  client_order_id: Optional[str] = None) -> OrderInfo:
        view = self._call(self._client._order_view, row)
        info = self._order_info(view, symbol=symbol, client_order_id=client_order_id,
                                venue_status=str(row.get("status") or ""))
        if info.client_order_id is None:            # a trigger order this adapter placed under a caller's id
            bound = self._bound_client_id(info.symbol, info.venue_order_id)
            return replace(info, client_order_id=bound) if bound else info
        return info


__all__ = ["BingXAdapter"]
