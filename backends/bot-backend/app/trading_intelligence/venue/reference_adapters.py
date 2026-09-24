"""Broker-neutral Forex and dated-futures economic adapters (Sections 17.20,
17.21).

These parse a NORMALIZED broker-metadata payload shape (documented per
adapter below) that a real broker integration -- OANDA instruments/pricing/
financing, MT bridge symbol info, IBKR contract details -- would populate.
No such integration has passed the venue economic contract suite against a
real venue yet, so both adapters resolve to UNVALIDATED at runtime
(``venue/registry.py``) and their economics fail the Section 13 cost-quality
gate. They exist so the ONE CATI cost contract is proven for Forex and
futures semantics: no USDT assumption, no perpetual funding, native swap/
rollover and per-contract commission, sessions, expiry and multipliers.
"""
from __future__ import annotations

from typing import Any, Optional

from app.trading_intelligence.contracts.instrument import FUTURE, FUTURES, FX, SPOT
from app.trading_intelligence.contracts.venue_economics import (
    CarryObservation, CarrySource, ExecutionCapabilities, FeeModel, FeeObservation, FeeSource, FinancingObservation,
    FinancingSource, FundingObservation, FundingSource, InstrumentMetadata, SwapUnit, VenueReasonCode,
)
from app.trading_intelligence.venue.adapter import BaseVenueEconomicAdapter, BookQuote, DepthBook

R = VenueReasonCode


def _f(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def _i(v: Any) -> Optional[int]:
    try:
        return int(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


class _NormalizedPayloadAdapter(BaseVenueEconomicAdapter):
    """Shared parsing of the normalized ``quote`` / ``depth`` / ``capabilities`` payloads."""

    def book_quote(self, raw) -> Optional[BookQuote]:
        q = raw.payloads.get("quote")
        if not isinstance(q, dict) or _f(q.get("bid")) is None or _f(q.get("ask")) is None:
            return None
        return BookQuote(_f(q["bid"]), _f(q["ask"]), _f(q.get("bid_qty")), _f(q.get("ask_qty")),
                         _i(q.get("time")) or raw.captured_at)

    def depth_book(self, raw) -> Optional[DepthBook]:
        d = raw.payloads.get("depth")
        if not isinstance(d, dict):
            return None
        return DepthBook(tuple((float(p), float(q)) for p, q in d.get("bids") or ()),
                         tuple((float(p), float(q)) for p, q in d.get("asks") or ()),
                         _i(d.get("time")) or raw.captured_at)

    def _capabilities(self, raw, meta: Optional[InstrumentMetadata]) -> Optional[ExecutionCapabilities]:
        c = raw.payloads.get("capabilities")
        if not isinstance(c, dict) or meta is None:
            return None  # an undeclared capability is not a supported one
        order_types = tuple(sorted(str(o).upper() for o in c.get("order_types") or ()))
        return ExecutionCapabilities(
            supported_order_types=order_types, supports_market="MARKET" in order_types,
            supports_limit="LIMIT" in order_types, supports_stop="STOP" in order_types,
            supports_stop_market="STOP_MARKET" in order_types, supports_post_only=bool(c.get("post_only", False)),
            supports_reduce_only=bool(c.get("reduce_only", False)), supports_partial_close=bool(c.get("partial_close", False)),
            supports_native_oco=bool(c.get("native_oco", False)), supports_hedge_mode=bool(c.get("hedge_mode", False)),
            supports_one_way_mode=bool(c.get("one_way_mode", True)), tick_size=meta.tick_size, step_size=meta.step_size,
            minimum_quantity=meta.minimum_quantity, minimum_notional=meta.minimum_notional,
            contract_multiplier=meta.contract_multiplier, margin_modes=tuple(c.get("margin_modes") or ()),
            settlement_currency=meta.settlement_currency,
            supported_time_in_force=tuple(sorted(str(t).upper() for t in c.get("time_in_force") or ())),
            venue_symbol=meta.venue_symbol, source=f"{self.adapter_id}:capabilities:{self.adapter_version}",
        )

    def describe_execution_capabilities(self, request, raw):
        return self._capabilities(raw, self.resolve_instrument_metadata(request, raw))


class ForexEconomicAdapter(_NormalizedPayloadAdapter):
    """Normalized FX payloads::

        instrument: {symbol, base, quote, tick_size, pip_size, step_size, min_qty,
                     min_notional?, lot_size, as_of?}
        quote:      {bid, ask, bid_qty?, ask_qty?, time}
        commission: {model: SPREAD_ONLY}
                  | {model: SPREAD_PLUS_COMMISSION, per_lot, lot_units, currency}
                  | {model: SPREAD_PLUS_COMMISSION, rate}
                    (+ account_specific: bool, as_of?)
        financing:  {swap_long, swap_short, unit: ANNUAL_RATE|PRICE_POINTS_PER_UNIT,
                     point_size?, triple_weekday?, days?, as_of?}
        conversion_fee_rate?, capabilities?
    """

    adapter_id = "forex_reference"
    broker = "FX_BROKER"
    venue_id = "FX_REFERENCE"
    adapter_version = "1.0.0"
    supported_asset_classes = (FX,)

    def resolve_instrument_metadata(self, request, raw) -> Optional[InstrumentMetadata]:
        i = raw.payloads.get("instrument")
        if not isinstance(i, dict) or not i.get("base") or not i.get("quote"):
            return None
        tick, step, min_qty = _f(i.get("tick_size")), _f(i.get("step_size")), _f(i.get("min_qty"))
        if not tick or not step or min_qty is None:
            return None
        base, quote = str(i["base"]).upper(), str(i["quote"]).upper()
        return InstrumentMetadata(
            venue_symbol=str(i.get("symbol", raw.venue_symbol)).upper(), canonical_symbol=f"{base}/{quote}:{SPOT}",
            asset_class=FX, contract_type=SPOT, base_currency=base, quote_currency=quote, settlement_currency=quote,
            tick_size=tick, step_size=step, minimum_quantity=min_qty, minimum_notional=_f(i.get("min_notional")),
            contract_multiplier=1.0, pip_size=_f(i.get("pip_size")), lot_size=_f(i.get("lot_size")),
            source=f"{self.adapter_id}:instrument", as_of=_i(i.get("as_of")),
        )

    def estimate_fees(self, request, raw) -> FeeObservation:
        c = raw.payloads.get("commission")
        if not isinstance(c, dict) or not c.get("model"):
            # we cannot tell a spread-only account from a commission account: fail closed
            return FeeObservation(fee_model=FeeModel.SPREAD_PLUS_COMMISSION.value, source=FeeSource.UNAVAILABLE.value,
                                  reason_codes=(R.FEE_UNAVAILABLE.value,))
        source = (FeeSource.OBSERVED_ACCOUNT_TIER if c.get("account_specific") else FeeSource.BROKER_METADATA).value
        conv = _f(raw.payloads.get("conversion_fee_rate"))
        model = str(c["model"]).upper()
        if model == FeeModel.SPREAD_ONLY.value:
            return FeeObservation(fee_model=model, source=source, currency_conversion_fee_rate=conv,
                                  observed_at=_i(c.get("as_of")))
        return FeeObservation(
            fee_model=FeeModel.SPREAD_PLUS_COMMISSION.value, source=source,
            commission_per_contract=_f(c.get("per_lot")), commission_contract_size=_f(c.get("lot_units")) or 1.0,
            commission_currency=(str(c["currency"]).upper() if c.get("currency") else None),
            commission_to_quote_rate=_f(c.get("to_quote_rate")),
            broker_commission_rate=_f(c.get("rate")), currency_conversion_fee_rate=conv, observed_at=_i(c.get("as_of")),
        )

    def estimate_funding_or_financing(self, request, raw):
        # FX has no perpetual funding. Holding cost is swap/rollover, native semantics.
        funding = FundingObservation(applicable=False, source=FundingSource.NOT_APPLICABLE.value)
        fin = raw.payloads.get("financing")
        if not isinstance(fin, dict) or _f(fin.get("swap_long")) is None or _f(fin.get("swap_short")) is None:
            return funding, FinancingObservation(applicable=True, source=FinancingSource.UNAVAILABLE.value,
                                                 reason_codes=(R.FINANCING_UNAVAILABLE.value,))
        unit = str(fin.get("unit", SwapUnit.ANNUAL_RATE.value)).upper()
        return funding, FinancingObservation(
            applicable=True, source=FinancingSource.BROKER_SWAP_RATES.value, swap_long=_f(fin["swap_long"]),
            swap_short=_f(fin["swap_short"]), swap_unit=unit, point_size=_f(fin.get("point_size")),
            triple_swap_weekday=_i(fin.get("triple_weekday")),
            financing_days_of_week=tuple(int(d) for d in fin.get("days", (0, 1, 2, 3, 4))),
            observed_at=_i(fin.get("as_of")),
        )


class DatedFuturesEconomicAdapter(_NormalizedPayloadAdapter):
    """Normalized dated-futures payloads::

        contract:   {symbol, root, expiry_ms, multiplier, tick_size, tick_value,
                     currency, min_qty, step, as_of?}
        quote/depth as above; session_status: OPEN|THIN|CLOSED
        commission: {per_contract, exchange_fee?, clearing_fee?, currency,
                     account_specific?, as_of?}
        reference_spot: {price, time}   (for basis / carry)
        capabilities?
    """

    adapter_id = "dated_futures_reference"
    broker = "FUTURES_BROKER"
    venue_id = "FUTURES_REFERENCE"
    adapter_version = "1.0.0"
    supported_asset_classes = (FUTURES,)

    def resolve_instrument_metadata(self, request, raw) -> Optional[InstrumentMetadata]:
        c = raw.payloads.get("contract")
        if not isinstance(c, dict) or not c.get("root") or not c.get("currency"):
            return None
        tick, mult = _f(c.get("tick_size")), _f(c.get("multiplier"))
        step, min_qty = _f(c.get("step")) or 1.0, _f(c.get("min_qty")) or 1.0
        if not tick or not mult:
            return None
        root, ccy = str(c["root"]).upper(), str(c["currency"]).upper()
        return InstrumentMetadata(
            venue_symbol=str(c.get("symbol", raw.venue_symbol)).upper(), canonical_symbol=f"{root}/{ccy}:{FUTURE}",
            asset_class=FUTURES, contract_type=FUTURE, base_currency=root, quote_currency=ccy, settlement_currency=ccy,
            tick_size=tick, step_size=step, minimum_quantity=min_qty, minimum_notional=None, contract_multiplier=mult,
            tick_value=_f(c.get("tick_value")), expiry_ms=_i(c.get("expiry_ms")),
            source=f"{self.adapter_id}:contract", as_of=_i(c.get("as_of")),
        )

    def estimate_fees(self, request, raw) -> FeeObservation:
        c = raw.payloads.get("commission")
        if not isinstance(c, dict) or _f(c.get("per_contract")) is None:
            fallback = self.policy.fallback_commission_per_contract.get(FUTURES)
            if fallback is None:
                return FeeObservation(fee_model=FeeModel.PER_CONTRACT.value, source=FeeSource.UNAVAILABLE.value,
                                      reason_codes=(R.FEE_UNAVAILABLE.value,))
            contract = raw.payloads.get("contract") or {}
            return FeeObservation(
                fee_model=FeeModel.PER_CONTRACT.value, source=FeeSource.CONSERVATIVE_CONFIGURED_FALLBACK.value,
                commission_per_contract=fallback, commission_currency=str(contract.get("currency", "")).upper() or None,
                reason_codes=(R.FEE_TIER_UNKNOWN.value, R.FEE_FALLBACK_USED.value))
        source = (FeeSource.OBSERVED_ACCOUNT_TIER if c.get("account_specific") else FeeSource.BROKER_METADATA).value
        return FeeObservation(
            fee_model=FeeModel.PER_CONTRACT.value, source=source, commission_per_contract=_f(c["per_contract"]),
            exchange_fee_per_contract=_f(c.get("exchange_fee")), clearing_fee_per_contract=_f(c.get("clearing_fee")),
            commission_currency=(str(c["currency"]).upper() if c.get("currency") else None),
            commission_to_quote_rate=_f(c.get("to_quote_rate")), observed_at=_i(c.get("as_of")))

    def estimate_funding_or_financing(self, request, raw):
        return (FundingObservation(applicable=False, source=FundingSource.NOT_APPLICABLE.value),
                FinancingObservation(applicable=False, source=FinancingSource.NOT_APPLICABLE.value))

    def estimate_carry_basis(self, request, raw) -> CarryObservation:
        c = raw.payloads.get("contract") or {}
        expiry = _i(c.get("expiry_ms"))
        tte = (expiry - request.decision_time) if expiry is not None else None
        spot = raw.payloads.get("reference_spot")
        book = self.book_quote(raw)
        fut_px = (book.best_bid + book.best_ask) / 2.0 if book else None
        if not isinstance(spot, dict) or _f(spot.get("price")) is None or fut_px is None:
            return CarryObservation(applicable=True, source=CarrySource.UNAVAILABLE.value, expiry_ms=expiry,
                                    time_to_expiry_ms=tte, futures_price=fut_px, reason_codes=(R.CARRY_UNAVAILABLE.value,))
        spot_px = _f(spot["price"])
        return CarryObservation(applicable=True, source=CarrySource.OBSERVED_BASIS.value, expiry_ms=expiry,
                                time_to_expiry_ms=tte, reference_spot_price=spot_px, futures_price=fut_px,
                                basis_absolute=fut_px - spot_px, observed_at=_i(spot.get("time")))


__all__ = ["ForexEconomicAdapter", "DatedFuturesEconomicAdapter"]
