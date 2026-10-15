"""Bybit V5 linear contracts behind the venue adapter contract (Step 4.1).

A translation layer around ``app.exchange.bybit.client.BybitClient``. The raw client is not changed: it keeps
signing and its paginated reads.

Most calls go through the raw client's own transport (``_request_v5`` and ``_pages``) and its own order
normaliser (``_order_view``) rather than through its convenience methods, for three reasons that are all
about not losing information:

* ``_request_v5`` returns HTTP 200 answers that carry a failure in ``retCode``. Several convenience methods
  do not look at it (``set_leverage``, ``cancel_order``) or fold it into text. The adapter reads ``retCode``
  itself, so a refusal is never taken for success.
* the convenience views drop the venue's verbatim order status, the position's stop fields and the kind of
  an execution (trade, funding, settlement);
* ``place_order`` reports every acknowledged order as ``NEW``. The adapter reads the order back instead and
  says ``UNKNOWN`` when that read fails.

Every endpoint and field used here is one the raw client or ``app.execution.perpetual_protection`` already
sends or reads, with these exceptions, taken from the venue's documentation and not exercised against it:
``slTriggerBy``/``tpTriggerBy`` = ``MarkPrice`` (the repository only sends ``LastPrice``), a stop cleared with
``stopLoss`` = ``"0"``, the ``execType``, ``isMaker`` and ``orderLinkId`` fields of an execution, and the
``markPrice``, ``takeProfit`` and ``updatedTime`` fields of a position.

Protective stops are the position's own stop (``POST /v5/position/trading-stop``, ``tpslMode=Full``), as the
raw client's ``place_protection`` and ``update_protection`` do. Consequences:

* the stop closes the whole position, there is one per position and it is changed in place (``amend_stop``);
* the venue keeps no order id and no client order id for it. Its ``venue_order_id`` is ``POSITION_SL:<symbol>``
  (``POSITION_TP:`` for a take-profit), read from the position's ``stopLoss`` field;
* a request for a stop the position already carries at the same trigger is that stop; at another trigger it
  is ``STOP_ALREADY_EXISTS``, never a silent overwrite;
* the venue's own rows for these stops in the open-order list (``stopOrderType`` StopLoss / TakeProfit) are
  left out of ``open_orders`` so that each stop is reported once.

Error codes: ``34040`` and the wording of the leverage answer are handled in the raw client; every other
entry of ``_CODES`` comes from the venue's published error list only. Nothing was verified against the venue
in this work.
"""
from __future__ import annotations

import re
from dataclasses import replace
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from app.exchange.instruments import collect_cursor_pages, parse_bybit_instrument

from . import contract as c
from .contract import (AccountBalance, AdapterError, FeeRates, FillInfo, FundingInfo, InstrumentRules, OrderInfo,
                       OrderRequest, PositionInfo)
from .factory import AdapterBase, decimal_text, number, positive

_POSITION_SL, _POSITION_TP = "POSITION_SL:", "POSITION_TP:"
#: stopOrderType of the venue's own open-order rows for a whole-position stop
_POSITION_STOP_ROWS = ("StopLoss", "TakeProfit")
#: executions that change a position; funding and settlement rows of the same list are not fills
_FILL_TYPES = (None, "", "Trade", "AdlTrade", "BustTrade")
_HTTP = re.compile(r"Bybit HTTP (\d{3}): (.*)", re.DOTALL)
_RET_CODE = re.compile(r"retCode=(-?\d+) ?(.*)", re.DOTALL)
_NOT_MODIFIED_STOP = 34040              # repo: bybit/client.py update_protection
_NOT_MODIFIED_LEVERAGE = 110043         # doc; the raw client recognises the same answer by its "not modified" text
_MARGIN_MODES = {0: "CROSSED", 1: "ISOLATED"}


class BybitAdapter(AdapterBase):
    venue = c.BYBIT
    _CID_MAX_LENGTH = 36                # repo: bybit/client.py place_order sends orderLinkId[:36]
    _RATE_SCOPE = "UID"                 # doc: the venue's rate-limit headers describe the account's budget
    _CODES: Mapping[int, str] = {       # all doc (Bybit V5 error list); none exercised against the venue
        10002: c.TIMESTAMP_OUT_OF_SYNC,             # request time outside the receive window
        10003: c.AUTH_FAILED,                       # API key is invalid
        10004: c.AUTH_FAILED,                       # signature error
        10005: c.PERMISSION_DENIED,                 # permission denied for this key
        10010: c.PERMISSION_DENIED,                 # request address is not in the key's allow list
        10006: c.RATE_LIMITED,                      # too many visits
        10018: c.RATE_LIMITED,                      # exceeded the address rate limit
        10009: c.BANNED,                            # address has been banned
        110001: c.ORDER_NOT_FOUND,                  # order does not exist
        110004: c.INSUFFICIENT_MARGIN,              # wallet balance is insufficient
        110007: c.INSUFFICIENT_MARGIN,              # available balance is insufficient
        110012: c.INSUFFICIENT_MARGIN,              # insufficient available balance
        110017: c.REDUCE_ONLY_REJECTED,             # reduce-only rule not satisfied
        110072: c.DUPLICATE_CLIENT_ORDER_ID,        # orderLinkId is duplicate
    }
    #: doc: HTTP 403 is the venue's answer to an address over its limit (or from a place it does not serve)
    _STATUS_CODES: Mapping[int, str] = {401: c.AUTH_FAILED, 403: c.BANNED}
    _OUTCOME_UNKNOWN = frozenset({10000, 10016})    # doc: server timeout, server error
    _CAPABILITIES: Mapping[str, bool] = {
        "native_stop_market": True, "native_take_profit": True, "reduce_only": True, "close_position_stop": True,
        "stop_amend_in_place": True, "multiple_stops_per_position": False, "one_way_mode": True, "hedge_mode": False,
        "user_stream": False, "position_stop_endpoint": True, "quantity_stop": False, "stop_client_order_id": False}
    _NOTES: Tuple[str, ...] = (
        "the stop is the position's own stop (trading-stop, Full): one per position, changed in place",
        "the venue keeps no id for that stop: venue_order_id is POSITION_SL:<symbol>; persist it",
        "a stop needs an open position and disappears with it",
        "a stop for PART of a position is not carried (the raw client only sets whole-position stops); a stop "
        "request whose quantity is the whole open position is set as the position's stop",
        "hedge mode is not carried: orders are sent with positionIdx 0",
        "account totals are the unified account's USD-valued totals, labelled USDT as the raw client does",
        "the raw client discards response headers: retry_after_s and the used budget are never known",
        "private user stream authenticates before subscribing; demo and live use separate hosts",
    )

    # ------------------------------------------------------------------------------------ failure surfaces
    def _classify(self, exc: BaseException, mutation: bool) -> Optional[AdapterError]:
        text = str(exc)
        answer = _HTTP.search(text)
        if answer:                                  # the raw client keeps the status and the text, not the code
            return self._fail(int(answer.group(1)), None, answer.group(2), mutation)
        coded = _RET_CODE.search(text)
        if coded:
            return self._fail(None, int(coded.group(1)), coded.group(2), mutation)
        if "discovery incomplete" in text:
            return self._error(c.MALFORMED_RESPONSE, "the venue's paginated answer did not end")
        return None

    def _ask(self, method: str, path: str, payload: Mapping[str, Any], *, ok: Tuple[int, ...] = (0,)) -> Mapping[str, Any]:
        """One signed request; the ``result`` of an answer whose ``retCode`` is acceptable."""
        mutation = method != "GET"
        answer = self._call(self._client._request_v5, method, path, dict(payload), mutation=mutation)
        code = answer.get("retCode") if isinstance(answer, Mapping) else None
        if not isinstance(code, int) or isinstance(code, bool):
            raise self._unconfirmed("retCode") if mutation else self._error(c.MALFORMED_RESPONSE, f"{path}: no retCode")
        if code not in ok:
            raise self._fail(None, code, str(answer.get("retMsg") or ""), mutation)
        result = answer.get("result")
        return result if isinstance(result, Mapping) else {}

    def _pages(self, path: str, params: Mapping[str, Any]) -> List[Mapping[str, Any]]:
        rows = self._rows(self._call(self._client._pages, path, dict(params)), path)
        return [self._mapping(r, path) for r in rows]

    # ------------------------------------------------------------------------------------------ read only
    def instrument_rules(self, symbols: Optional[Sequence[str]] = None) -> Mapping[str, InstrumentRules]:
        def page(cursor: Optional[str]) -> Tuple[list, Optional[str]]:
            result = self._client._instruments_page("linear", cursor)
            return result.get("list") or [], result.get("nextPageCursor")

        records = self._call(collect_cursor_pages, page, source="bybit instruments-info")
        return self._publish((self._rules(
            r, self._parsed(parse_bybit_instrument, r, category="linear", venue=self.venue), base=r.get("baseCoin"),
            quote=r.get("quoteCoin"), venue_status=str(r.get("status") or "")) for r in records if isinstance(r, Mapping)),
            symbols)

    def balance(self) -> AccountBalance:
        wallet = None
        for account_type in ("UNIFIED", "CONTRACT"):    # as the raw client: an older account answers under CONTRACT
            rows = self._ask("GET", "/v5/account/wallet-balance", {"accountType": account_type}).get("list")
            if isinstance(rows, list) and rows:
                wallet = rows[0]
                break
        if wallet is None:
            raise self._error(c.MALFORMED_RESPONSE, "the venue reported no wallet for this account")
        return AccountBalance(
            venue=self.venue, margin_asset="USDT", wallet_balance=self._need(wallet, "totalWalletBalance", "wallet"),
            unrealized_pnl=self._need(wallet, "totalPerpUPL", "wallet"), equity=self._need(wallet, "totalEquity", "wallet"),
            available_balance=self._need(wallet, "totalAvailableBalance", "wallet"), observed_at_ms=self._now())

    def positions(self, symbol: Optional[str] = None) -> Tuple[PositionInfo, ...]:
        out = []
        for row in self._position_rows(self._to_venue(symbol) if symbol else None):
            signed = self._signed_size(row)
            if signed != 0:
                index = number(row.get("positionIdx"))
                out.append(self._position(
                    row.get("symbol"), signed, entry=row.get("avgPrice"), mark=row.get("markPrice"),
                    unrealized=row.get("unrealisedPnl"), leverage=row.get("leverage"),
                    margin_mode=_MARGIN_MODES.get(number(row.get("tradeMode"))),
                    position_mode=None if index is None else ("ONE_WAY" if index == 0 else "HEDGE")))
        return tuple(out)

    def open_orders(self, symbol: Optional[str] = None) -> Tuple[OrderInfo, ...]:
        venue_symbol = self._to_venue(symbol) if symbol else None
        scope = {"symbol": venue_symbol} if venue_symbol else {"settleCoin": "USDT"}
        out: List[OrderInfo] = []
        for order_filter in ("Order", "StopOrder", "tpslOrder"):
            for row in self._pages("/v5/order/realtime", {"category": "linear", "orderFilter": order_filter, **scope}):
                if row.get("stopOrderType") not in _POSITION_STOP_ROWS:     # reported once, from the position
                    out.append(self._row_info(row))
        return tuple(out + self._position_stops(self._position_rows(venue_symbol)))

    def order(self, symbol: str, *, client_order_id: Optional[str] = None,
              venue_order_id: Optional[str] = None) -> OrderInfo:
        self._identity(client_order_id, venue_order_id)
        canonical, venue_symbol = c.canonical_symbol(symbol), self._to_venue(symbol)
        target = str(venue_order_id) if venue_order_id else self._bound(client_order_id, canonical)
        if target and target.startswith((_POSITION_SL, _POSITION_TP)):
            for stop in self._position_stops(self._position_rows(venue_symbol)):
                if stop.venue_order_id == target:
                    return stop
            raise self._error(c.ORDER_NOT_FOUND, "the position carries no such stop")
        identity = {"orderId": target} if venue_order_id else {"orderLinkId": self._client_id(client_order_id)}
        return self._row_info(self._find(venue_symbol, **identity), canonical, client_order_id)

    def fills(self, symbol: str, *, since_ms: Optional[int] = None,
              venue_order_id: Optional[str] = None) -> Tuple[FillInfo, ...]:
        if venue_order_id and str(venue_order_id).startswith((_POSITION_SL, _POSITION_TP)):
            raise self._unsupported("a position's stop has no order id at the venue: its execution is reported under "
                                    "an id this adapter cannot know; read fills by time")
        canonical, venue_symbol = c.canonical_symbol(symbol), self._to_venue(symbol)
        found: Dict[str, FillInfo] = {}
        for start, end in self._fill_windows(since_ms):
            for row in self._pages("/v5/execution/list", {"category": "linear", "symbol": venue_symbol, "limit": 100,
                                                          "startTime": start, "endTime": end}):
                if row.get("execType") not in _FILL_TYPES:
                    continue
                fee = number(row.get("execFee"))
                fill = self._fill_info(
                    canonical, trade_id=row.get("execId"), venue_order_id=row.get("orderId"),
                    client_order_id=row.get("orderLinkId"), side=row.get("side"), quantity=row.get("execQty"),
                    price=row.get("execPrice"), fee=fee, fee_asset=row.get("feeCurrency") if fee is not None else None,
                    realized_pnl=None, is_maker=row.get("isMaker") if isinstance(row.get("isMaker"), bool) else None,
                    time=row.get("execTime"))
                found[fill.trade_id] = fill
        return tuple(f for f in found.values() if venue_order_id is None or f.venue_order_id == str(venue_order_id))

    def fee_rates(self, symbol: str) -> FeeRates:
        row = self._call(self._client.get_trading_fee_rates, self._to_venue(symbol))
        return FeeRates(venue=self.venue, symbol=c.canonical_symbol(symbol), maker=self._need(row, "maker", "fee-rate"),
                        taker=self._need(row, "taker", "fee-rate"), source="ACCOUNT")

    def funding(self, symbol: str) -> FundingInfo:
        rows = self._rows(self._ask("GET", "/v5/market/tickers", {"category": "linear", "symbol": self._to_venue(symbol)})
                          .get("list"), "tickers")
        if not rows:
            raise self._error(c.MALFORMED_RESPONSE, "the venue returned no ticker for this symbol")
        row = self._mapping(rows[0], "tickers")
        upcoming = positive(row.get("nextFundingTime"))
        # the interval is an instrument rule (fundingInterval), not part of the ticker
        return FundingInfo(venue=self.venue, symbol=c.canonical_symbol(symbol), rate=number(row.get("fundingRate")),
                           next_funding_ms=int(upcoming) if upcoming is not None else None, interval_minutes=None,
                           mark_price=positive(row.get("markPrice")), observed_at_ms=self._now())

    # ------------------------------------------------------------------------------------------ mutations
    def set_leverage(self, symbol: str, leverage: int) -> int:
        value = str(int(leverage))
        self._ask("POST", "/v5/position/set-leverage", {"category": "linear", "symbol": self._to_venue(symbol),
                                                        "buyLeverage": value, "sellLeverage": value},
                  ok=(0, _NOT_MODIFIED_LEVERAGE))
        return int(leverage)                        # the venue echoes nothing; it accepted this value

    def place_order(self, request: OrderRequest) -> OrderInfo:
        venue_symbol, venue_id = self._to_venue(request.symbol), self._client_id(request.client_order_id)
        if request.protective:
            return self._place_stop(request, venue_symbol)
        self._entry_only_gtc(request)

        def lookup() -> OrderInfo:
            return self._row_info(self._find(venue_symbol, orderLinkId=venue_id), request.symbol, request.client_order_id)

        def send() -> OrderInfo:
            payload: Dict[str, Any] = {
                "category": "linear", "symbol": venue_symbol, "side": "Buy" if request.side == c.BUY else "Sell",
                "orderType": "Market" if request.order_type == c.MARKET else "Limit",
                "qty": decimal_text(request.quantity), "positionIdx": 0, "orderLinkId": venue_id}
            if request.order_type == c.LIMIT:
                payload["price"], payload["timeInForce"] = decimal_text(request.price), "GTC"
            if request.reduce_only:
                payload["reduceOnly"] = True
            order_id = self._ask("POST", "/v5/order/create", payload).get("orderId")
            if order_id in (None, ""):
                raise self._unconfirmed("order id")
            # the venue only acknowledges: its state is read, not assumed
            return self._read_back(
                lambda: self._row_info(self._find(venue_symbol, orderId=str(order_id)), request.symbol,
                                       request.client_order_id),
                self._acknowledged(request, str(order_id), status=c.UNKNOWN, venue_status="ACKNOWLEDGED"))

        return self._idempotent(request, lookup, send)

    def _place_stop(self, request: OrderRequest, venue_symbol: str) -> OrderInfo:
        stop_loss = request.order_type == c.STOP_MARKET
        field, prefix = ("stopLoss", _POSITION_SL) if stop_loss else ("takeProfit", _POSITION_TP)
        venue_id = prefix + venue_symbol
        position = self._open_position(venue_symbol)
        self._whole_position_stop(request, self._signed_size(position) if position is not None else 0.0)
        resting = positive(position.get(field))
        if resting is not None:
            if decimal_text(resting) != decimal_text(request.stop_price):
                raise self._error(c.STOP_ALREADY_EXISTS, "the position already carries a stop at another trigger: amend it")
            # the venue keeps no id for it: the same position and the same trigger is the same stop
            self._bind(request.client_order_id, request.symbol, venue_id)
            return next(s for s in self._position_stops([position]) if s.venue_order_id == venue_id)
        trigger_by = "MarkPrice" if request.working_type == "MARK_PRICE" else "LastPrice"
        self._bind(request.client_order_id, request.symbol, venue_id)
        try:
            self._ask("POST", "/v5/position/trading-stop",
                      {"category": "linear", "symbol": venue_symbol, "tpslMode": "Full", "positionIdx": 0,
                       field: decimal_text(request.stop_price), "slTriggerBy" if stop_loss else "tpTriggerBy": trigger_by},
                      ok=(0, _NOT_MODIFIED_STOP))
        except AdapterError as failed:
            if not failed.uncertain:                # refused: nothing rests under this id
                self._unbind(request.symbol, venue_id)
            raise
        return self._acknowledged(request, venue_id, status=c.NEW, venue_status="ACCEPTED", whole_position=True)

    def amend_stop(self, symbol: str, *, client_order_id: str, new_stop_price: float) -> OrderInfo:
        canonical, venue_symbol = c.canonical_symbol(symbol), self._to_venue(symbol)
        take_profit = (self._bound(client_order_id, canonical) or "").startswith(_POSITION_TP)
        field, prefix = ("takeProfit", _POSITION_TP) if take_profit else ("stopLoss", _POSITION_SL)
        venue_id = prefix + venue_symbol
        position = self._open_position(venue_symbol)
        if position is None or positive(position.get(field)) is None:
            raise self._error(c.ORDER_NOT_FOUND, "the position carries no stop to amend")
        # only the trigger is sent, so that the price basis chosen when the stop was placed is not replaced
        self._ask("POST", "/v5/position/trading-stop",
                  {"category": "linear", "symbol": venue_symbol, "tpslMode": "Full", "positionIdx": 0,
                   field: decimal_text(new_stop_price)}, ok=(0, _NOT_MODIFIED_STOP))
        self._bind(client_order_id, canonical, venue_id)
        amended = next(s for s in self._position_stops([position]) if s.venue_order_id == venue_id)
        return replace(amended, stop_price=float(new_stop_price), venue_status="ACCEPTED", updated_at_ms=self._now())

    def cancel_order(self, symbol: str, *, client_order_id: Optional[str] = None,
                     venue_order_id: Optional[str] = None) -> OrderInfo:
        found = self.order(symbol, client_order_id=client_order_id, venue_order_id=venue_order_id)
        if found.final:
            return found
        venue_symbol, target = self._to_venue(symbol), str(found.venue_order_id or "")
        if target.startswith((_POSITION_SL, _POSITION_TP)):
            field = "stopLoss" if target.startswith(_POSITION_SL) else "takeProfit"
            self._ask("POST", "/v5/position/trading-stop",
                      {"category": "linear", "symbol": venue_symbol, "tpslMode": "Full", "positionIdx": 0, field: "0"},
                      ok=(0, _NOT_MODIFIED_STOP))
            self._unbind(found.symbol, target)
            return replace(found, status=c.CANCELED, venue_status="CANCEL_ACCEPTED", updated_at_ms=self._now())
        if not target:
            raise self._error(c.MALFORMED_RESPONSE, "the venue returned an order without an id; it cannot be cancelled")
        self._ask("POST", "/v5/order/cancel", {"category": "linear", "symbol": venue_symbol, "orderId": target})
        # the venue only acknowledges a cancel: the order's state is read, not assumed
        return self._read_back(
            lambda: self._row_info(self._find(venue_symbol, orderId=target), found.symbol, found.client_order_id),
            replace(found, status=c.UNKNOWN, venue_status="CANCEL_ACKNOWLEDGED", updated_at_ms=self._now()))

    # ------------------------------------------------------------------------------------- orders and stops
    def _find(self, venue_symbol: str, **identity: str) -> Mapping[str, Any]:
        """The venue's record of one order: among the active ones first, then in the history."""
        for path in ("/v5/order/realtime", "/v5/order/history"):
            result = self._ask("GET", path, {"category": "linear", "symbol": venue_symbol, **identity})
            rows = self._rows(result.get("list"), path)
            if rows:
                return self._mapping(rows[0], path)
        raise self._error(c.ORDER_NOT_FOUND, "the venue holds no such order")

    def _row_info(self, row: Mapping[str, Any], symbol: Optional[str] = None,
                  client_order_id: Optional[str] = None) -> OrderInfo:
        view = self._call(self._client._order_view, row)
        return self._order_info(view, symbol=symbol, client_order_id=client_order_id,
                                venue_status=str(row.get("orderStatus") or ""))

    def _position_rows(self, venue_symbol: Optional[str]) -> List[Mapping[str, Any]]:
        params = {"category": "linear", "settleCoin": "USDT"}
        if venue_symbol:
            params["symbol"] = venue_symbol
        return self._pages("/v5/position/list", params)

    def _signed_size(self, row: Mapping[str, Any]) -> float:
        size = self._need(row, "size", "position")
        return size if row.get("side") == "Buy" else -size if row.get("side") == "Sell" else 0.0

    def _open_position(self, venue_symbol: str) -> Optional[Mapping[str, Any]]:
        """The one-way position of the symbol, None when flat. A hedge-mode position is refused."""
        held = [r for r in self._position_rows(venue_symbol) if self._signed_size(r) != 0]
        if any(number(r.get("positionIdx")) not in (None, 0) for r in held):
            raise self._error(c.POSITION_MODE_MISMATCH, "the account holds a hedge-mode position; one-way is required")
        return held[0] if held else None

    def _position_stops(self, rows: Sequence[Mapping[str, Any]]) -> List[OrderInfo]:
        """The whole-position stops the given position rows carry, as resting orders."""
        out: List[OrderInfo] = []
        for row in rows:
            signed = self._signed_size(row)
            if signed == 0:
                continue
            symbol = c.canonical_symbol(row.get("symbol"))
            for prefix, field, kind in ((_POSITION_SL, "stopLoss", c.STOP_MARKET),
                                        (_POSITION_TP, "takeProfit", c.TAKE_PROFIT_MARKET)):
                trigger = positive(row.get(field))
                if trigger is not None:
                    venue_id = prefix + str(row.get("symbol"))
                    out.append(OrderInfo(
                        venue=self.venue, symbol=symbol, client_order_id=self._bound_client_id(symbol, venue_id),
                        venue_order_id=venue_id, status=c.NEW, venue_status="POSITION_ATTRIBUTE",
                        side=c.SELL if signed > 0 else c.BUY, order_type=kind, quantity=None, filled_quantity=0.0,
                        average_price=None, stop_price=trigger, reduce_only=False, close_position=True,
                        updated_at_ms=int(number(row.get("updatedTime")) or 0)))
        return out


__all__ = ["BybitAdapter"]
