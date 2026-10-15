"""Binance USD-M futures behind the venue adapter contract (Step 4.1).

A translation layer around ``app.exchange.binance.client.BinanceFuturesClient``. The raw client is not
changed: it keeps signing, its time synchronisation and its own bounded handling of rate limits on reads.

Where the adapter uses a public method of the raw client and where it uses the client's signed transport:

* Public methods: ``server_time``, ``exchange_info``, ``account``, ``position_risk``, ``open_orders``,
  ``get_algo_orders``, ``get_order``, ``get_order_by_client_order_id``, ``user_trades``, ``get_funding``,
  ``set_leverage``, ``place_market_order``.
* Signed transport, with an endpoint and fields this repository already sends or names:
  ``POST /fapi/v1/algoOrder`` (protective stops, as ``app.execution.production_protection`` sends them),
  ``DELETE /fapi/v1/algoOrder``, ``DELETE /fapi/v1/order`` (the public ``cancel_order`` swallows every error
  into ``False``, which hides an uncertain outcome), ``GET /fapi/v1/commissionRate`` (named in
  ``app.trading_intelligence.venue.binance``).
* Signed transport, with a shape taken from the venue's documentation because the raw client has no working
  path. Neither has been exercised against the venue:

  - a LIMIT order (``price``, ``timeInForce=GTC``): the raw ``place_order`` sends a LIMIT order without its
    price;
  - ``workingType=MARK_PRICE`` on a stop: the repository only sends ``CONTRACT_PRICE``;
  - the open conditional orders of every symbol (``GET /fapi/v1/openAlgoOrders`` without ``symbol``): the raw
    ``get_algo_orders`` needs a symbol. If the venue insists on one it answers with an error, which the
    adapter raises; it never returns a list with the stops silently left out.

Protective stops are Binance conditional ("algo") orders bound to the whole position (``closePosition``).
They carry the caller's id as ``clientAlgoId`` and live in their own id space, so their ``venue_order_id`` is
written ``algo:<algoId>``. One limit follows from what the raw client exposes and is stated in
``capabilities().notes``: only OPEN conditional orders can be read back. One that fired or was cancelled is
reported as ``ORDER_NOT_FOUND``, exactly like one that never existed.

Error codes: every entry of ``_CODES`` names its evidence. ``repo`` means this repository already handles
that code (file named); ``doc`` means the venue's published error list only. Nothing was verified against
the venue in this work.
"""
from __future__ import annotations

import re
from dataclasses import replace
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from app.exchange.binance.client import BinanceRateLimited, rate_limit_backoff_remaining
from app.exchange.instruments import parse_binance_symbol

from . import contract as c
from .contract import (AccountBalance, AdapterError, FeeRates, FillInfo, FundingInfo, InstrumentRules, OrderInfo,
                       OrderRequest, PositionInfo)
from .factory import AdapterBase, decimal_text, json_error, number, positive

_ALGO = "algo:"
_PAGE = 1000
_HTTP = re.compile(r"Binance HTTP (\d{3}): (.*)", re.DOTALL)
_INNER_STATUS = re.compile(r"HTTP (\d{3})|\b(\d{3}) (?:Client|Server) Error")
#: the raw transport's stand-in for venue code -4130 (it returns this instead of raising)
_DUPLICATE_STOP_MARKER = "DUPLICATE_4130"
_MARGIN_MODES = {"cross": "CROSSED", "crossed": "CROSSED", "isolated": "ISOLATED"}
_POSITION_MODES = {"BOTH": "ONE_WAY", "LONG": "HEDGE", "SHORT": "HEDGE"}


class BinanceAdapter(AdapterBase):
    venue = c.BINANCE
    _CID_MAX_LENGTH = 36                # newClientOrderId: ^[\.A-Z\:/a-z0-9_-]{1,36}$ (doc); clientAlgoId assumed equal
    _RATE_SCOPE = "IP"
    _CODES: Mapping[int, str] = {
        -1003: c.RATE_LIMITED,                      # repo: tests/test_binance_rate_limits.py, fill_resolution
        -1015: c.RATE_LIMITED,                      # repo: fill_resolution.TRANSIENT_NOT_PROCESSED_CODES (order rate)
        -1021: c.TIMESTAMP_OUT_OF_SYNC,             # repo: binance/client.py _signed_request
        -1022: c.AUTH_FAILED,                       # doc: signature for this request is not valid
        -2014: c.AUTH_FAILED,                       # doc: API-key format invalid
        -2015: c.AUTH_FAILED,                       # repo: tests/test_binance_rate_limits.py (key, IP or permissions)
        -2018: c.INSUFFICIENT_MARGIN,               # repo: execution/executor.py
        -2019: c.INSUFFICIENT_MARGIN,               # repo: execution/executor.py
        -4164: c.MIN_NOTIONAL,                      # repo: exchange/catalog_refresh.py
        -4003: c.INVALID_QUANTITY,                  # repo: exchange/catalog_refresh.py
        -4005: c.INVALID_QUANTITY,                  # repo: exchange/catalog_refresh.py
        -4023: c.INVALID_QUANTITY,                  # repo: exchange/catalog_refresh.py
        -4014: c.INVALID_PRICE,                     # repo: exchange/catalog_refresh.py
        -1121: c.INSTRUMENT_UNKNOWN,                # repo: exchange/catalog_refresh.py, main.py
        -4140: c.INSTRUMENT_NOT_TRADING,            # repo: exchange/catalog_refresh.py
        -2013: c.ORDER_NOT_FOUND,                   # repo: fill_resolution.ORDER_DOES_NOT_EXIST
        -2011: c.ORDER_NOT_FOUND,                   # doc: cancel rejected, unknown order sent
        -4111: c.DUPLICATE_CLIENT_ORDER_ID,         # repo: fill_resolution.DUPLICATE_CLIENT_ID_CODES
        -4115: c.DUPLICATE_CLIENT_ORDER_ID,         # repo: fill_resolution.DUPLICATE_CLIENT_ID_CODES
        -4116: c.DUPLICATE_CLIENT_ORDER_ID,         # repo: fill_resolution.DUPLICATE_CLIENT_ID_CODES
        -2022: c.REDUCE_ONLY_REJECTED,              # repo: tests/test_binance_rate_limits.py
        -2021: c.STOP_WOULD_TRIGGER_IMMEDIATELY,    # repo: execution/executor.py
        -4130: c.STOP_ALREADY_EXISTS,               # repo: binance/client.py _signed_request
        -4061: c.POSITION_MODE_MISMATCH,            # doc: order's position side does not match the user's setting
        -4028: c.LEVERAGE_NOT_ALLOWED,              # doc: leverage is not valid
    }
    #: -10xx are server and network conditions; only these are documented as a refusal of the request itself
    #: (repo: fill_resolution._DEFINITIVE_10XX). For the rest the venue does not say whether it acted.
    _OUTCOME_UNKNOWN = frozenset(range(-1099, -999)) - {-1002, -1003, -1013, -1014, -1015, -1020, -1021, -1022, -1023}
    _CAPABILITIES: Mapping[str, bool] = {
        "native_stop_market": True, "native_take_profit": True, "reduce_only": True, "close_position_stop": True,
        "stop_amend_in_place": False, "multiple_stops_per_position": False, "one_way_mode": True, "hedge_mode": False,
        "user_stream": False, "position_stop_endpoint": False, "quantity_stop": False, "stop_client_order_id": True}
    _NOTES: Tuple[str, ...] = (
        "stops are conditional orders bound to the whole position; venue_order_id is written algo:<algoId>",
        "a second closePosition stop of the same kind and side is refused by the venue (-4130)",
        "a conditional order that is no longer open cannot be read back: ORDER_NOT_FOUND",
        "UNVERIFIED: open_orders() without a symbol lists conditional orders through a request the raw client never sends",
        "a stop for PART of a position is not carried (the repository sends closePosition stops only); a stop "
        "request whose quantity is the whole open position is placed as that closePosition stop",
        "hedge mode is not carried: orders are sent for a one-way account",
        "private user stream uses the /private/ws route; venue certification remains separately required",
        "the raw client itself repeats a rate-limited READ within a small bounded sleep budget; mutations never",
    )

    def __init__(self, client: Any, environment: str, **kw: Any) -> None:
        super().__init__(client, environment, **kw)
        self._ban_until_ms: Optional[int] = None

    # ------------------------------------------------------------------------------------ failure surfaces
    def _classify(self, exc: BaseException, mutation: bool) -> Optional[AdapterError]:
        text = str(exc)
        if isinstance(exc, BinanceRateLimited):     # a read the client did not send: the venue said back off
            status = re.search(r"\(HTTP (\d{3})\)", text)
            return self._fail(int(status.group(1)) if status else 429, None, "the venue asked this address to back off",
                              mutation)
        answer = _HTTP.search(text)
        if answer:
            code, message = json_error(answer.group(2), "code", "msg")
            return self._fail(int(answer.group(1)), code, message, mutation)
        if text.startswith("Binance request failed after retries"):
            status = _INNER_STATUS.search(text)
            if status:
                return self._fail(int(status.group(1) or status.group(2)), None, "", mutation)
            return self._unanswered(mutation, "no answer to a public read")
        if isinstance(exc, ValueError) and text.startswith("Missing BINANCE_API_KEY"):
            return self._error(c.AUTH_FAILED, "the connected account has no usable credential")
        return None

    def _fail(self, status: Optional[int], code: Optional[int], message: str, mutation: bool) -> AdapterError:
        if status == 418:
            remaining = rate_limit_backoff_remaining(self._client.base_url)
            if remaining > 0:
                self._ban_until_ms = self._now() + int(remaining * 1000)
        return super()._fail(status, code, message, mutation)

    def _retry_after_s(self) -> Optional[float]:
        remaining = rate_limit_backoff_remaining(self._client.base_url)
        return remaining if remaining > 0 else None

    def _banned_until_ms(self) -> Optional[int]:
        return self._ban_until_ms if self._ban_until_ms is not None and self._ban_until_ms > self._now() else None

    def _used_weight(self) -> Optional[float]:
        return number(getattr(self._client, "last_used_weight_1m", None))

    def _refuse_stop_marker(self, answer: Any) -> None:
        if isinstance(answer, Mapping) and answer.get("orderId") == _DUPLICATE_STOP_MARKER:
            raise self._error(c.STOP_ALREADY_EXISTS, "a closePosition order of this kind and side already rests",
                              venue_code="-4130", http_status=400)

    # ------------------------------------------------------------------------------------------ read only
    def instrument_rules(self, symbols: Optional[Sequence[str]] = None) -> Mapping[str, InstrumentRules]:
        info = self._call(self._client.exchange_info)
        records = self._rows(info.get("symbols") if isinstance(info, Mapping) else None, "exchangeInfo")
        return self._publish((self._symbol_rules(r) for r in records if isinstance(r, Mapping)), symbols)

    def _symbol_rules(self, record: Mapping[str, Any]) -> Optional[InstrumentRules]:
        filters = {f.get("filterType"): f for f in record.get("filters") or [] if isinstance(f, Mapping)}
        return self._rules(record, self._parsed(parse_binance_symbol, record, venue=self.venue),
                           base=record.get("baseAsset"), quote=record.get("quoteAsset"),
                           venue_status=str(record.get("status") or ""),
                           market_max=positive((filters.get("MARKET_LOT_SIZE") or {}).get("maxQty")))

    def balance(self) -> AccountBalance:
        account = self._call(self._client.account)
        return AccountBalance(
            venue=self.venue, margin_asset="USDT",      # the totals of /fapi/v2/account, as the raw client labels them
            wallet_balance=self._need(account, "totalWalletBalance", "account"),
            unrealized_pnl=self._need(account, "totalUnrealizedProfit", "account"),
            equity=self._need(account, "totalMarginBalance", "account"),
            available_balance=self._need(account, "availableBalance", "account"), observed_at_ms=self._now())

    def positions(self, symbol: Optional[str] = None) -> Tuple[PositionInfo, ...]:
        rows = self._rows(self._call(self._client.position_risk, self._to_venue(symbol) if symbol else None), "positionRisk")
        out = []
        for row in rows:
            amount = self._need(row, "positionAmt", "position")
            if amount != 0:
                out.append(self._position(
                    row.get("symbol"), amount, entry=row.get("entryPrice"), mark=row.get("markPrice"),
                    unrealized=row.get("unRealizedProfit"), leverage=row.get("leverage"),
                    margin_mode=_MARGIN_MODES.get(str(row.get("marginType") or "").lower()),
                    position_mode=_POSITION_MODES.get(str(row.get("positionSide") or "").upper())))
        return tuple(out)

    def open_orders(self, symbol: Optional[str] = None) -> Tuple[OrderInfo, ...]:
        canonical = c.canonical_symbol(symbol) if symbol else None
        venue_symbol = self._to_venue(symbol) if symbol else None
        regular = self._rows(self._call(self._client.open_orders, venue_symbol), "openOrders")
        return tuple([self._order_info(self._mapping(o, "openOrders"), symbol=canonical) for o in regular]
                     + [self._algo_info(o, canonical) for o in self._open_algos(venue_symbol)])

    def order(self, symbol: str, *, client_order_id: Optional[str] = None,
              venue_order_id: Optional[str] = None) -> OrderInfo:
        self._identity(client_order_id, venue_order_id)
        canonical, venue_symbol = c.canonical_symbol(symbol), self._to_venue(symbol)
        if venue_order_id:
            wanted = str(venue_order_id)
            if wanted.startswith(_ALGO):
                return self._algo_lookup(venue_symbol, canonical, algo_id=wanted[len(_ALGO):])
            if not wanted.isdigit():
                raise self._error(c.ORDER_NOT_FOUND, "not an order id of this venue")
            found = self._call(self._client.get_order, venue_symbol, wanted)
            return self._order_info(self._mapping(found, "order"), symbol=canonical)
        venue_id = self._client_id(client_order_id)
        try:
            found = self._call(self._client.get_order_by_client_order_id, venue_symbol, venue_id)
            return self._order_info(self._mapping(found, "order"), symbol=canonical, client_order_id=client_order_id)
        except AdapterError as missing:
            if missing.code != c.ORDER_NOT_FOUND:
                raise
        return self._algo_lookup(venue_symbol, canonical, client_id=venue_id, client_order_id=client_order_id)

    def fills(self, symbol: str, *, since_ms: Optional[int] = None,
              venue_order_id: Optional[str] = None) -> Tuple[FillInfo, ...]:
        if venue_order_id and str(venue_order_id).startswith(_ALGO):
            raise self._unsupported("a conditional order's fills belong to the order it created when it fired: "
                                    "read fills by time")
        canonical, venue_symbol = c.canonical_symbol(symbol), self._to_venue(symbol)
        found: Dict[str, FillInfo] = {}
        for start, end in self._fill_windows(since_ms):
            rows = self._rows(self._call(self._client.user_trades, venue_symbol, start, end, _PAGE), "userTrades")
            if len(rows) >= _PAGE:
                raise self._unsupported("the fill page is full and may be cut short: read a shorter range")
            for row in rows:
                row = self._mapping(row, "userTrades")
                fee = number(row.get("commission"))
                fill = self._fill_info(
                    canonical, trade_id=row.get("id"), venue_order_id=row.get("orderId"), client_order_id=None,
                    side=row.get("side"), quantity=row.get("qty"), price=row.get("price"), fee=fee,
                    fee_asset=row.get("commissionAsset") if fee is not None else None,
                    realized_pnl=number(row.get("realizedPnl")),
                    is_maker=row.get("maker") if isinstance(row.get("maker"), bool) else None, time=row.get("time"))
                found[fill.trade_id] = fill
        return tuple(f for f in found.values() if venue_order_id is None or f.venue_order_id == str(venue_order_id))

    def fee_rates(self, symbol: str) -> FeeRates:
        row = self._call(self._client._signed_get, "/fapi/v1/commissionRate", {"symbol": self._to_venue(symbol)})
        return FeeRates(venue=self.venue, symbol=c.canonical_symbol(symbol),
                        maker=self._need(row, "makerCommissionRate", "commissionRate"),
                        taker=self._need(row, "takerCommissionRate", "commissionRate"), source="ACCOUNT")

    def funding(self, symbol: str) -> FundingInfo:
        row = self._mapping(self._call(self._client.get_funding, self._to_venue(symbol)), "premiumIndex")
        upcoming = positive(row.get("nextFundingTime"))
        # the interval is published in a separate public list (fundingInfo) that this adapter does not read
        return FundingInfo(venue=self.venue, symbol=c.canonical_symbol(symbol), rate=number(row.get("fundingRate")),
                           next_funding_ms=int(upcoming) if upcoming is not None else None, interval_minutes=None,
                           mark_price=positive(row.get("markPrice")), observed_at_ms=self._now())

    # ------------------------------------------------------------------------------------------ mutations
    def set_leverage(self, symbol: str, leverage: int) -> int:
        answer = self._call(self._client.set_leverage, self._to_venue(symbol), int(leverage), mutation=True)
        accepted = number(answer.get("leverage")) if isinstance(answer, Mapping) else None
        if accepted is None:
            raise self._unconfirmed("leverage")
        return int(accepted)

    def place_order(self, request: OrderRequest) -> OrderInfo:
        venue_symbol, venue_id = self._to_venue(request.symbol), self._client_id(request.client_order_id)
        if request.protective:
            return self._place_stop(request, venue_symbol, venue_id)
        self._entry_only_gtc(request)

        def lookup() -> OrderInfo:
            found = self._call(self._client.get_order_by_client_order_id, venue_symbol, venue_id)
            return self._order_info(self._mapping(found, "order"), symbol=request.symbol,
                                    client_order_id=request.client_order_id)

        def send() -> OrderInfo:
            quantity = decimal_text(request.quantity)
            if request.order_type == c.MARKET:
                answer = self._call(self._client.place_market_order, venue_symbol, request.side, quantity,
                                    request.reduce_only, venue_id, mutation=True)
            else:
                params = {"symbol": venue_symbol, "side": request.side, "type": "LIMIT", "quantity": quantity,
                          "price": decimal_text(request.price), "timeInForce": "GTC", "newOrderRespType": "RESULT",
                          "newClientOrderId": venue_id}
                if request.reduce_only:
                    params["reduceOnly"] = "true"
                answer = self._call(self._client._signed_post, "/fapi/v1/order", params, mutation=True)
            self._refuse_stop_marker(answer)
            if not isinstance(answer, Mapping) or answer.get("orderId") in (None, ""):
                raise self._unconfirmed("order id")
            return self._order_info(answer, symbol=request.symbol, client_order_id=request.client_order_id)

        return self._idempotent(request, lookup, send)

    def _place_stop(self, request: OrderRequest, venue_symbol: str, venue_id: str) -> OrderInfo:
        def lookup() -> OrderInfo:
            return self._algo_lookup(venue_symbol, request.symbol, client_id=venue_id,
                                     client_order_id=request.client_order_id)

        def send() -> OrderInfo:
            if not request.close_position:              # a quantity: only the whole open position can be carried
                held = [r for r in self._rows(self._call(self._client.position_risk, venue_symbol), "positionRisk")
                        if self._need(r, "positionAmt", "position") != 0]
                if any(_POSITION_MODES.get(str(r.get("positionSide") or "").upper()) == "HEDGE" for r in held):
                    raise self._error(c.POSITION_MODE_MISMATCH, "the account holds a hedge-mode position")
                self._whole_position_stop(request, self._need(held[0], "positionAmt", "position") if held else 0.0)
            params = {"algoType": "CONDITIONAL", "symbol": venue_symbol, "side": request.side,
                      "type": request.order_type, "triggerPrice": decimal_text(request.stop_price),
                      "closePosition": "true", "workingType": request.working_type, "clientAlgoId": venue_id}
            answer = self._call(self._client._signed_post, "/fapi/v1/algoOrder", params, mutation=True)
            self._refuse_stop_marker(answer)
            if not isinstance(answer, Mapping) or answer.get("algoId") in (None, ""):
                raise self._unconfirmed("conditional order id")
            # accepted by the venue as a resting conditional order; the caller reads protection back
            return self._acknowledged(request, _ALGO + str(answer["algoId"]), status=c.NEW,
                                      venue_status=str(answer.get("algoStatus") or "ACCEPTED"), whole_position=True)

        return self._idempotent(request, lookup, send)

    def cancel_order(self, symbol: str, *, client_order_id: Optional[str] = None,
                     venue_order_id: Optional[str] = None) -> OrderInfo:
        found = self.order(symbol, client_order_id=client_order_id, venue_order_id=venue_order_id)
        if found.final:
            return found
        venue_symbol, target = self._to_venue(symbol), str(found.venue_order_id or "")
        if target.startswith(_ALGO):
            self._call(self._client._signed_delete, "/fapi/v1/algoOrder",
                       {"symbol": venue_symbol, "algoId": target[len(_ALGO):]}, mutation=True)
            return replace(found, status=c.CANCELED, venue_status="CANCEL_ACCEPTED", updated_at_ms=self._now())
        if not target.isdigit():
            raise self._error(c.MALFORMED_RESPONSE, "the venue returned an order without an id; it cannot be cancelled")
        answer = self._call(self._client._signed_delete, "/fapi/v1/order",
                            {"symbol": venue_symbol, "orderId": int(target)}, mutation=True)
        if isinstance(answer, Mapping) and answer.get("status"):
            return self._order_info(answer, symbol=found.symbol, client_order_id=found.client_order_id)
        return replace(found, status=c.UNKNOWN, venue_status="CANCEL_ACKNOWLEDGED", updated_at_ms=self._now())

    # ----------------------------------------------------------------------------------- conditional orders
    def _open_algos(self, venue_symbol: Optional[str]) -> list:
        if venue_symbol:
            return self._rows(self._call(self._client.get_algo_orders, venue_symbol, raise_on_error=True), "openAlgoOrders")
        # Every symbol: the same endpoint without its symbol. The raw method needs one, so this goes through the
        # signed transport and unwraps the answer the way that method does.
        answer = self._call(self._client._signed_get, "/fapi/v1/openAlgoOrders", {})
        if isinstance(answer, Mapping):
            answer = answer.get("algoOrders", answer.get("orders"))
        return self._rows(answer, "openAlgoOrders")

    def _algo_info(self, row: Any, symbol: Optional[str], client_order_id: Optional[str] = None) -> OrderInfo:
        row = self._mapping(row, "openAlgoOrders")
        if row.get("algoId") in (None, ""):
            raise self._error(c.MALFORMED_RESPONSE, "a conditional order without an algoId")
        view = {"symbol": row.get("symbol"), "side": row.get("side"), "type": row.get("type") or row.get("orderType"),
                "stopPrice": row.get("triggerPrice") or row.get("stopPrice"), "closePosition": row.get("closePosition"),
                "reduceOnly": row.get("reduceOnly"), "origQty": row.get("quantity"),
                "clientOrderId": row.get("clientAlgoId"), "updateTime": row.get("updateTime"),
                "status": "NEW"}                    # the venue lists it among its OPEN conditional orders
        return self._order_info(view, symbol=symbol, client_order_id=client_order_id,
                                venue_order_id=_ALGO + str(row["algoId"]), venue_status=str(row.get("algoStatus") or "OPEN"))

    def _algo_lookup(self, venue_symbol: str, symbol: str, *, algo_id: Optional[str] = None,
                     client_id: Optional[str] = None, client_order_id: Optional[str] = None) -> OrderInfo:
        for row in self._open_algos(venue_symbol):
            if isinstance(row, Mapping) and ((algo_id is not None and str(row.get("algoId")) == algo_id)
                                             or (client_id is not None and row.get("clientAlgoId") == client_id)):
                return self._algo_info(row, symbol, client_order_id)
        raise self._error(c.ORDER_NOT_FOUND, "no such order and no open conditional order with this identity "
                                             "(one that fired or was cancelled is no longer listed)")


__all__ = ["BinanceAdapter"]
