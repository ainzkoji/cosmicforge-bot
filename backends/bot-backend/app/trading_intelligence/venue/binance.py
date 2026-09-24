"""Binance USD-M economic adapter (Section 17.19) -- the first real venue.

Reuses the EXISTING ``BinanceFuturesClient`` (no second exchange client).
The collector calls only its PUBLIC, unsigned market-data methods
(exchangeInfo, bookTicker, premiumIndex, depth, fundingInfo) and copies a
whitelisted subset of each payload: the client's key/secret are never read,
and no signed endpoint is ever called from CATI.

What is real today (verified against demo-fapi.binance.com, 2026-09-23):

* instrument metadata: tickSize / stepSize / minQty / MIN_NOTIONAL, orderTypes,
  timeInForce (GTX = post-only), marginAsset
* top of book: bookTicker bid/ask/qty with venue ``time``
* depth: /fapi/v1/depth levels with transaction time ``T``
* funding: premiumIndex lastFundingRate / nextFundingTime / mark / index;
  fundingInfo fundingIntervalHours
* fees: the account's commission tier (/fapi/v1/commissionRate) is a SIGNED
  endpoint and is deliberately not called -> conservative VIP-0 fallback
  (FEE_TIER_UNKNOWN), never zero, never the cheapest tier.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple

from app.trading_intelligence.contracts.instrument import CRYPTO, FUTURE, PERPETUAL
from app.trading_intelligence.contracts.venue_economics import (
    CarryObservation, CarrySource, ExecutionCapabilities, FeeModel, FeeObservation, FeeSource, FinancingObservation,
    FinancingSource, FundingObservation, FundingSource, InstrumentMetadata, VenueReasonCode,
)
from app.trading_intelligence.venue.adapter import BaseVenueEconomicAdapter, BookQuote, DepthBook, VenueRawSnapshot

R = VenueReasonCode

_DEFAULT_FUNDING_INTERVAL_MS = 8 * 3_600_000  # Binance's documented standard interval
_SYMBOL_KEYS = ("symbol", "pair", "contractType", "deliveryDate", "status", "baseAsset", "quoteAsset", "marginAsset",
                "pricePrecision", "quantityPrecision", "filters", "orderTypes", "timeInForce")
_BOOK_KEYS = ("symbol", "bidPrice", "bidQty", "askPrice", "askQty", "time")
_PREMIUM_KEYS = ("symbol", "markPrice", "indexPrice", "lastFundingRate", "nextFundingTime", "interestRate", "time")
_DEPTH_KEYS = ("bids", "asks", "T", "E")
_FUNDING_INFO_KEYS = ("symbol", "fundingIntervalHours", "adjustedFundingRateCap", "adjustedFundingRateFloor")


def _f(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def _i(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def _subset(payload: Any, keys) -> Optional[dict]:
    return {k: payload.get(k) for k in keys if k in payload} if isinstance(payload, dict) else None


class BinanceUsdmEconomicAdapter(BaseVenueEconomicAdapter):
    adapter_id = "binance_usdm"
    broker = "BINANCE"
    venue_id = "BINANCE_USDM"
    adapter_version = "1.0.0"
    supported_asset_classes = (CRYPTO,)

    # -- metadata ------------------------------------------------------------------------
    @staticmethod
    def _info(raw: VenueRawSnapshot) -> Optional[dict]:
        info = raw.payloads.get("exchange_info_symbol")
        if not isinstance(info, dict) or str(info.get("symbol", "")).upper() != raw.venue_symbol.upper():
            return None
        return info

    @staticmethod
    def _filter(info: dict, kind: str) -> dict:
        return next((f for f in info.get("filters") or () if f.get("filterType") == kind), {})

    def resolve_instrument_metadata(self, request, raw) -> Optional[InstrumentMetadata]:
        info = self._info(raw)
        if info is None:
            return None
        from app.universe.identity import FUTURE as U_FUTURE, PERP as U_PERP, canonical_instrument

        perpetual = info.get("contractType") == "PERPETUAL"
        canonical = canonical_instrument(info.get("baseAsset", ""), info.get("quoteAsset", ""),
                                         U_PERP if perpetual else U_FUTURE)
        tick = _f(self._filter(info, "PRICE_FILTER").get("tickSize"))
        lot = self._filter(info, "LOT_SIZE")
        step, min_qty = _f(lot.get("stepSize")), _f(lot.get("minQty"))
        if not tick or not step or min_qty is None:
            return None  # precision unknown: never guessed
        notional = self._filter(info, "MIN_NOTIONAL")
        return InstrumentMetadata(
            venue_symbol=str(info["symbol"]).upper(), canonical_symbol=canonical.canonical_id, asset_class=CRYPTO,
            contract_type=PERPETUAL if perpetual else FUTURE, base_currency=str(info.get("baseAsset", "")),
            quote_currency=str(info.get("quoteAsset", "")), settlement_currency=info.get("marginAsset"),
            tick_size=tick, step_size=step, minimum_quantity=min_qty,
            minimum_notional=_f(notional.get("notional", notional.get("minNotional"))),
            contract_multiplier=1.0,  # USD-M: price is per venue unit (even for 1000PEPE)
            expiry_ms=None if perpetual else _i(info.get("deliveryDate")),
            source="binance:/fapi/v1/exchangeInfo", as_of=_i(raw.payloads.get("exchange_info_as_of")),
        )

    # -- fees ------------------------------------------------------------------------------
    def estimate_fees(self, request, raw) -> FeeObservation:
        observed = raw.payloads.get("commission_rate")  # only if a safe account service supplied it
        if isinstance(observed, dict):
            maker, taker = _f(observed.get("makerCommissionRate")), _f(observed.get("takerCommissionRate"))
            if taker is not None and taker > 0 and maker is not None and maker >= 0:
                return FeeObservation(fee_model=FeeModel.PERCENT_NOTIONAL.value,
                                      source=FeeSource.OBSERVED_ACCOUNT_TIER.value, maker_fee_rate=maker,
                                      taker_fee_rate=taker, observed_at=_i(observed.get("as_of")))
            refused: Tuple[str, ...] = (R.ZERO_FEE_REFUSED.value,)
        else:
            refused = ()
        maker, taker = self.policy.fallback_fee_rates[CRYPTO]
        return FeeObservation(
            fee_model=FeeModel.PERCENT_NOTIONAL.value, source=FeeSource.CONSERVATIVE_CONFIGURED_FALLBACK.value,
            maker_fee_rate=maker, taker_fee_rate=taker, fee_tier="VIP0_CONSERVATIVE_FALLBACK",
            reason_codes=refused + (R.FEE_TIER_UNKNOWN.value, R.FEE_FALLBACK_USED.value))

    # -- book / depth ----------------------------------------------------------------------
    def book_quote(self, raw) -> Optional[BookQuote]:
        b = raw.payloads.get("book_ticker")
        if not isinstance(b, dict):
            return None
        bid, ask = _f(b.get("bidPrice")), _f(b.get("askPrice"))
        if bid is None or ask is None:
            return None
        return BookQuote(bid, ask, _f(b.get("bidQty")), _f(b.get("askQty")), _i(b.get("time")) or raw.captured_at)

    def depth_book(self, raw) -> Optional[DepthBook]:
        d = raw.payloads.get("depth")
        if not isinstance(d, dict):
            return None
        try:
            bids = tuple((float(p), float(q)) for p, q in d.get("bids") or ())
            asks = tuple((float(p), float(q)) for p, q in d.get("asks") or ())
        except (TypeError, ValueError):
            return None
        return DepthBook(bids, asks, _i(d.get("T")) or _i(d.get("E")) or raw.captured_at)

    def payload_timestamps(self, raw) -> Tuple[int, ...]:
        extra = [_i((raw.payloads.get("premium_index") or {}).get("time")), _i(raw.payloads.get("exchange_info_as_of"))]
        return super().payload_timestamps(raw) + tuple(t for t in extra if t is not None)

    # -- funding -----------------------------------------------------------------------------
    def estimate_funding_or_financing(self, request, raw):
        financing = FinancingObservation(applicable=False, source=FinancingSource.NOT_APPLICABLE.value)
        info = self._info(raw)
        if info is not None and info.get("contractType") != "PERPETUAL":
            return FundingObservation(applicable=False, source=FundingSource.NOT_APPLICABLE.value), financing
        p = raw.payloads.get("premium_index")
        if not isinstance(p, dict) or _f(p.get("lastFundingRate")) is None:
            return FundingObservation(applicable=True, source=FundingSource.UNAVAILABLE.value,
                                      reason_codes=(R.FUNDING_UNAVAILABLE.value,)), financing
        as_of = _i(p.get("time")) or raw.captured_at
        codes = [R.PREDICTED_FUNDING_UNAVAILABLE.value]
        interval, schedule = self._funding_interval(raw)
        if not schedule:
            codes.append(R.FUNDING_SCHEDULE_UNKNOWN.value)
        mark, index = _f(p.get("markPrice")), _f(p.get("indexPrice"))
        if mark is None or index is None:
            codes.append(R.MARK_INDEX_UNAVAILABLE.value)
        source = FundingSource.CURRENT_RATE_SCHEDULE.value
        if request.decision_time - as_of > self.policy.max_funding_age_ms:
            codes += [R.FUNDING_STALE.value, R.FUNDING_UNAVAILABLE.value]
            source = FundingSource.UNAVAILABLE.value
        return FundingObservation(
            applicable=True, source=source,
            # premiumIndex's lastFundingRate is the rate accruing toward nextFundingTime;
            # Binance publishes no separate forecast, so no predicted rate is fabricated.
            current_funding_rate=_f(p.get("lastFundingRate")), predicted_funding_rate=None,
            next_funding_time=_i(p.get("nextFundingTime")), funding_interval_ms=interval,
            mark_price=mark, index_price=index, observed_at=as_of, reason_codes=tuple(codes)), financing

    @staticmethod
    def _funding_interval(raw) -> Tuple[int, bool]:
        entries = raw.payloads.get("funding_info")
        if not isinstance(entries, (list, tuple)):
            return _DEFAULT_FUNDING_INTERVAL_MS, False
        for e in entries:
            if isinstance(e, dict) and str(e.get("symbol", "")).upper() == raw.venue_symbol.upper():
                hours = _f(e.get("fundingIntervalHours"))
                if hours and hours > 0:
                    return int(hours * 3_600_000), True
        # fundingInfo lists symbols with adjusted parameters; others use the standard 8h.
        return _DEFAULT_FUNDING_INTERVAL_MS, True

    def estimate_carry_basis(self, request, raw) -> CarryObservation:
        info = self._info(raw)
        if info is None or info.get("contractType") == "PERPETUAL":
            # perpetual holding cost is funding; mark/index basis stays in the funding observation
            return CarryObservation(applicable=False, source=CarrySource.NOT_APPLICABLE.value)
        return CarryObservation(applicable=True, source=CarrySource.UNAVAILABLE.value, reason_codes=(R.CARRY_UNAVAILABLE.value,))

    # -- capabilities -------------------------------------------------------------------------
    def describe_execution_capabilities(self, request, raw) -> Optional[ExecutionCapabilities]:
        info = self._info(raw)
        meta = self.resolve_instrument_metadata(request, raw)
        if info is None or meta is None:
            return None
        order_types = tuple(sorted(str(o) for o in info.get("orderTypes") or ()))
        tifs = tuple(sorted(str(t) for t in info.get("timeInForce") or ()))
        return ExecutionCapabilities(
            supported_order_types=order_types, supports_market="MARKET" in order_types,
            supports_limit="LIMIT" in order_types, supports_stop="STOP" in order_types,
            supports_stop_market="STOP_MARKET" in order_types, supports_post_only="GTX" in tifs,
            # USD-M venue-documented order parameters (not per-symbol in exchangeInfo):
            supports_reduce_only=True, supports_partial_close=True, supports_native_oco=False,
            supports_hedge_mode=True, supports_one_way_mode=True,
            tick_size=meta.tick_size, step_size=meta.step_size, minimum_quantity=meta.minimum_quantity,
            minimum_notional=meta.minimum_notional, contract_multiplier=meta.contract_multiplier,
            margin_modes=("CROSSED", "ISOLATED"), settlement_currency=meta.settlement_currency,
            supported_time_in_force=tifs, venue_symbol=meta.venue_symbol,
            source=f"binance:/fapi/v1/exchangeInfo+usdm_documented:{self.adapter_version}",
        )


# ---------------------------------------------------------------------------
# Credential-free collector over the EXISTING client
# ---------------------------------------------------------------------------
_FUNDING_INFO_TTL_S = 600
_funding_info_cache: Dict[int, Tuple[float, list]] = {}
_cache_lock = threading.Lock()


def _funding_info(client: Any) -> Optional[list]:
    fn = getattr(client, "funding_info", None)
    if not callable(fn):
        return None
    with _cache_lock:
        hit = _funding_info_cache.get(id(client))
        if hit is not None and time.time() - hit[0] < _FUNDING_INFO_TTL_S:
            return hit[1]
    data = fn()
    rows = [_subset(e, _FUNDING_INFO_KEYS) for e in data or () if isinstance(e, dict)]
    with _cache_lock:
        _funding_info_cache[id(client)] = (time.time(), rows)
    return rows


def collect_binance_raw(client: Any, venue_symbol: str, *, clock: Optional[Callable[[], int]] = None) -> VenueRawSnapshot:
    """Capture public market data for one symbol. Every step is independent
    and failure-tolerant: a failed call becomes an explicit COLLECTION_ERROR
    and the corresponding component falls back -- never a fabricated value."""
    clock = clock or (lambda: int(time.time() * 1000))
    sym = str(venue_symbol).upper()
    payloads: Dict[str, Any] = {}
    errors = []

    def attempt(name: str, fn: Callable[[], Any]) -> None:
        try:
            value = fn()
            if value is not None:
                payloads[name] = value
        except Exception as exc:  # recorded, never raised into the runner
            errors.append(f"{name}:{type(exc).__name__}")

    def symbol_info():
        info = client.exchange_info_cached()
        payloads["exchange_info_as_of"] = _i((info or {}).get("serverTime"))
        match = next((s for s in (info or {}).get("symbols", ()) if str(s.get("symbol", "")).upper() == sym), None)
        return _subset(match, _SYMBOL_KEYS)

    attempt("exchange_info_symbol", symbol_info)
    attempt("book_ticker", lambda: _subset(client.book_ticker(sym), _BOOK_KEYS))
    attempt("premium_index", lambda: _subset(client.mark_price(sym), _PREMIUM_KEYS))
    if callable(getattr(client, "depth", None)):
        attempt("depth", lambda: _subset(client.depth(sym, limit=20), _DEPTH_KEYS))
    def funding_rows():
        rows = _funding_info(client)
        # an empty match is meaningful (standard interval); None = not fetchable
        return None if rows is None else [e for e in rows if str(e.get("symbol", "")).upper() == sym]

    attempt("funding_info", funding_rows)
    codes = (R.COLLECTION_ERROR.value,) if errors else ()
    return VenueRawSnapshot(venue_symbol=sym, payloads=payloads, captured_at=clock(), reason_codes=codes)


__all__ = ["BinanceUsdmEconomicAdapter", "collect_binance_raw"]
