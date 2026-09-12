"""Broker universe adapters -- the only broker-specific code in the universe.

An adapter answers three questions about the connected account's venue, using
the account's own exchange client (so a demo account discovers the demo venue,
never mainnet):

* which instruments exist (``instruments``) -- slow-changing metadata;
* cheap batched market statistics (``market_stats``) -- one or two requests for
  the whole venue, never one request per symbol;
* how much request budget is left (``request_budget``), when the venue says.

Metrics a venue cannot supply are left ``None`` and ``capabilities`` says so.
Adding Bybit, OKX, Deribit or IBKR means adding an adapter here; nothing above
this module changes.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol

from app.universe.contracts import InstrumentMeta, MarketStats, Product
from app.universe.identity import FUTURE, PERP, canonical_instrument


class UniverseAdapterUnavailable(RuntimeError):
    """No universe adapter exists for this broker. Fails closed: no candidates."""


@dataclass(frozen=True)
class RequestBudget:
    used: int | None
    limit: int | None

    @property
    def fraction_used(self) -> float | None:
        if self.used is None or not self.limit:
            return None
        return float(self.used) / float(self.limit)


class UniverseAdapter(Protocol):
    venue: str

    def capabilities(self) -> Mapping[str, bool]: ...

    def instruments(self) -> list[InstrumentMeta]: ...

    def market_stats(self) -> dict[str, MarketStats]: ...

    def request_budget(self) -> RequestBudget: ...


def _positive(value: Any) -> float | None:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


class BinanceUsdmUniverseAdapter:
    """Binance USD-M futures, through the connected account's client."""

    venue = "binance_usdm"

    #: Delivery contracts expire; the bot's protection model assumes perpetuals.
    _PERPETUAL_TYPES = frozenset({"PERPETUAL", "TRADIFI_PERPETUAL"})

    def __init__(self, client: Any) -> None:
        self._client = client
        self._weight_limit: int | None = None

    def capabilities(self) -> Mapping[str, bool]:
        return {
            "quote_volume_24h": True,
            "trade_count_24h": True,
            "last_price": True,
            "spread_bps": True,
            "listing_age": True,
            "open_interest": False,      # per-symbol endpoint only; not fetched for ranking
            "order_book_depth": False,   # per-symbol endpoint only
            "volatility": False,         # measured from candles at evaluation, not at ranking
            "price_continuity": False,
        }

    # -- metadata ---------------------------------------------------------

    def instruments(self) -> list[InstrumentMeta]:
        info = self._client.exchange_info()
        for limit in info.get("rateLimits", []) or []:
            if limit.get("rateLimitType") == "REQUEST_WEIGHT" and limit.get("interval") == "MINUTE":
                try:
                    self._weight_limit = int(limit.get("limit")) * int(limit.get("intervalNum", 1) or 1)
                except (TypeError, ValueError):
                    pass
        out: list[InstrumentMeta] = []
        for s in info.get("symbols", []) or []:
            symbol = str(s.get("symbol") or "").upper()
            if not symbol:
                continue
            contract_type = str(s.get("contractType") or "").upper()
            if contract_type in self._PERPETUAL_TYPES:
                product, kind = Product.PERPETUAL, PERP
            elif any(t in contract_type for t in ("QUARTER", "MONTH", "WEEK", "DELIVER")):
                product, kind = Product.DELIVERY, FUTURE
            else:
                product, kind = Product.OTHER, PERP
            filters = {f.get("filterType"): f for f in s.get("filters", []) or []}
            price = filters.get("PRICE_FILTER", {})
            lot = filters.get("LOT_SIZE", {})
            notional = filters.get("MIN_NOTIONAL", {})
            delivery = s.get("deliveryDate")
            out.append(InstrumentMeta(
                venue=self.venue,
                venue_symbol=symbol,
                canonical=canonical_instrument(s.get("baseAsset", ""), s.get("quoteAsset", ""), kind),
                status=str(s.get("status") or ""),
                tradable=str(s.get("status") or "").upper() == "TRADING",
                product=product,
                underlying_type=(str(s.get("underlyingType")).upper() if s.get("underlyingType") else None),
                quote_asset=str(s.get("quoteAsset") or "").upper(),
                margin_asset=str(s.get("marginAsset") or s.get("quoteAsset") or "").upper(),
                tick_size=_positive(price.get("tickSize")),
                step_size=_positive(lot.get("stepSize")),
                min_qty=_positive(lot.get("minQty")),
                min_notional=_positive(notional.get("notional") or notional.get("minNotional")),
                listed_at_ms=int(s["onboardDate"]) if s.get("onboardDate") else None,
                expires_at_ms=int(delivery) if (product == Product.DELIVERY and delivery) else None,
            ))
        return out

    # -- batched market statistics -----------------------------------------

    def _get(self, path: str) -> Any:
        # max_retries=1: one Retry-After-respecting retry, then give up. A
        # universe refresh must never hold the runner (and the position
        # management behind it) in a long retry loop.
        return self._client._request("GET", path, max_retries=1)

    def market_stats(self) -> dict[str, MarketStats]:
        tickers = self._get("/fapi/v1/ticker/24hr") or []
        books = self._get("/fapi/v1/ticker/bookTicker") or []
        book_by = {str(b.get("symbol") or "").upper(): b for b in books if isinstance(b, dict)}
        out: dict[str, MarketStats] = {}
        for t in tickers:
            if not isinstance(t, dict):
                continue
            symbol = str(t.get("symbol") or "").upper()
            if not symbol:
                continue
            spread = None
            book = book_by.get(symbol)
            if book:
                bid, ask = _positive(book.get("bidPrice")), _positive(book.get("askPrice"))
                if bid and ask and ask >= bid:
                    spread = (ask - bid) / ((ask + bid) / 2.0) * 10_000.0
            try:
                count = int(t["count"]) if t.get("count") is not None else None
            except (TypeError, ValueError):
                count = None
            try:
                stats_time = int(t["closeTime"]) if t.get("closeTime") else None
            except (TypeError, ValueError):
                stats_time = None
            try:
                quote_volume = float(t["quoteVolume"]) if t.get("quoteVolume") is not None else None
            except (TypeError, ValueError):
                quote_volume = None
            out[symbol] = MarketStats(
                quote_volume_24h=quote_volume,
                trade_count_24h=count,
                last_price=_positive(t.get("lastPrice")),
                spread_bps=spread,
                stats_time_ms=stats_time,
                open_interest=None,
            )
        return out

    def request_budget(self) -> RequestBudget:
        used = getattr(self._client, "last_used_weight_1m", None)
        return RequestBudget(used=int(used) if used is not None else None, limit=self._weight_limit)


#: broker_type -> adapter factory. The registry is the only place a broker is named.
_ADAPTERS: dict[str, Callable[[Any], UniverseAdapter]] = {
    "binance": BinanceUsdmUniverseAdapter,
}


def register_adapter(broker_type: str, factory: Callable[[Any], UniverseAdapter]) -> None:
    _ADAPTERS[str(broker_type).strip().lower()] = factory


def adapter_for(broker_type: str, client: Any) -> UniverseAdapter:
    factory = _ADAPTERS.get(str(broker_type or "").strip().lower())
    if factory is None:
        raise UniverseAdapterUnavailable(
            f"no universe adapter for broker {broker_type!r}; supported: {sorted(_ADAPTERS)}"
        )
    return factory(client)


__all__ = [
    "BinanceUsdmUniverseAdapter",
    "RequestBudget",
    "UniverseAdapter",
    "UniverseAdapterUnavailable",
    "adapter_for",
    "register_adapter",
]
