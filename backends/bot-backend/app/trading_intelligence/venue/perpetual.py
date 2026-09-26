"""Bybit/BingX economics over existing normalized exchange clients.

No competing cost model or client. New adapters remain UNVALIDATED until
external environment evidence is recorded. No reference-feed payload is read.
"""
from dataclasses import fields
import math
import threading
import time
import weakref
from app.trading_intelligence.contracts.venue_economics import (
    FeeObservation, FundingObservation, FinancingObservation, InstrumentMetadata,
    SlippageObservation, VenueEconomicObservation,
)
from app.trading_intelligence.venue.adapter import BaseVenueEconomicAdapter, BookQuote, DepthBook, VenueRawSnapshot
from app.trading_intelligence.venue.policy import MultiAssetVenueCostPolicy


def number(value):
    try:
        v = float(value)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def fresh(stamp, now, ttl):
    return isinstance(stamp, (int, float)) and 0 <= now - stamp <= ttl


class PerpetualEconomicAdapter(BaseVenueEconomicAdapter):
    supported_asset_classes = ("CRYPTO", "FX")

    def __init__(self, policy=None, status_registry=None):
        super().__init__(policy or MultiAssetVenueCostPolicy(), status_registry)
        if not getattr(self.policy, "strict_required_components", False):
            raise ValueError("MULTI_ASSET_VERSIONED_POLICY_REQUIRED")

    def resolve_instrument_metadata(self, request, raw):
        info = raw.payloads.get("instrument")
        if not info or not info.api_tradable or info.contract_type != "PERPETUAL":
            return None
        if info.venue.lower() != self.broker.lower():
            return None
        if info.venue_symbol.upper() != raw.venue_symbol.upper():
            return None
        values = (info.tick_size, info.qty_step, info.min_qty, info.contract_multiplier)
        if any(number(v) is None or number(v) <= 0 for v in values) or not info.settlement_asset:
            return None
        key = info.to_instrument_key()
        return InstrumentMetadata(
            info.venue_symbol, key.canonical_symbol, info.asset_class, info.contract_type,
            info.base_currency, info.quote_currency, info.settlement_asset,
            info.tick_size, info.qty_step, info.min_qty, info.min_notional,
            contract_multiplier=info.contract_multiplier, source=f"{self.broker}:instrument_discovery",
            as_of=raw.payloads.get("metadata_as_of"))

    def estimate_fees(self, request, raw):
        fee = raw.payloads.get("account_fee") or {}
        codes = []
        if not request.user_id or not request.broker_account_id or (
            fee.get("user_id"), fee.get("broker_account_id"), fee.get("environment"), fee.get("venue_symbol")) != (
            request.user_id, request.broker_account_id, request.environment, raw.venue_symbol):
            codes.append("FEE_ACCOUNT_SCOPE_MISMATCH")
        if not fresh(fee.get("as_of"), request.decision_time, self.policy.max_fee_age_ms):
            codes.append("FEE_TIER_STALE_OR_UNAVAILABLE")
        maker, taker = number(fee.get("maker")), number(fee.get("taker"))
        if maker is None or taker is None or maker < 0 or taker <= 0:
            codes.append("FEE_TIER_UNAVAILABLE")
        if codes:
            return FeeObservation("PERCENT_NOTIONAL", "UNAVAILABLE", reason_codes=tuple(codes + ["FEE_UNAVAILABLE"]))
        return FeeObservation("PERCENT_NOTIONAL", "OBSERVED_ACCOUNT_TIER", maker, taker,
                              fee_tier="ACCOUNT_OBSERVED", observed_at=fee["as_of"])

    def depth_book(self, raw):
        data = raw.payloads.get("book") or {}
        try:
            bids = tuple((float(p), float(q)) for p, q in data["bids"])
            asks = tuple((float(p), float(q)) for p, q in data["asks"])
            if not bids or not asks or any(not math.isfinite(v) or v <= 0 for side in (bids, asks) for row in side for v in row):
                return None
            if list(bids) != sorted(bids, reverse=True) or list(asks) != sorted(asks) or bids[0][0] > asks[0][0]:
                return None
            ts = int(data["time"])
            return DepthBook(bids, asks, ts) if ts > 0 else None
        except (KeyError, TypeError, ValueError):
            return None

    def book_quote(self, raw):
        depth = self.depth_book(raw)
        if depth is None:
            return None
        return BookQuote(depth.bids[0][0], depth.asks[0][0], depth.bids[0][1], depth.asks[0][1], depth.as_of)

    def _slippage(self, request, raw, notes):
        depth = self.depth_book(raw)
        if (depth is None or not fresh(depth.as_of, request.decision_time, self.policy.max_depth_age_ms)
                or min(len(depth.bids), len(depth.asks)) < self.policy.min_reliable_depth_levels):
            notes.add("SLIPPAGE_MODEL_UNAVAILABLE")
            return SlippageObservation("UNAVAILABLE", reason_codes=("SLIPPAGE_MODEL_UNAVAILABLE",))
        return SlippageObservation("DEPTH_WALK", depth_bids=depth.bids, depth_asks=depth.asks, depth_as_of=depth.as_of)

    def estimate_funding_or_financing(self, request, raw):
        info = raw.payloads.get("instrument")
        data = raw.payloads.get("funding") or {}
        rate = number(data.get("fundingRate"))
        interval = getattr(info, "funding_interval_minutes", None)
        if interval is None and number(data.get("fundingIntervalHours")):
            interval = int(number(data["fundingIntervalHours"]) * 60)  # BingX: published on premiumIndex, not contracts
        nxt = number(data.get("nextFundingTime"))
        stamp = raw.payloads.get("funding_as_of")
        valid = (rate is not None and interval is not None and interval > 0 and nxt is not None
                 and nxt > request.decision_time and fresh(stamp, request.decision_time, self.policy.max_funding_age_ms))
        funding = FundingObservation(True, "CURRENT_RATE_SCHEDULE" if valid else "UNAVAILABLE",
            current_funding_rate=rate if valid else None, next_funding_time=int(nxt) if valid else None,
            funding_interval_ms=int(interval * 60_000) if valid else None,
            mark_price=number(data.get("markPrice")), index_price=number(data.get("indexPrice")),
            observed_at=stamp, reason_codes=() if valid else ("FUNDING_UNAVAILABLE",))
        return funding, FinancingObservation(False, "NOT_APPLICABLE")

    def observe(self, request, raw):
        obs = super().observe(request, raw)
        codes = list(obs.reason_codes)
        if request.instrument_key.venue.lower() != self.broker.lower() or request.instrument_key.venue_symbol != raw.venue_symbol:
            codes.append("INSTRUMENT_MAPPING_MISMATCH")
        if not fresh(raw.payloads.get("metadata_as_of"), request.decision_time, self.policy.max_metadata_age_ms):
            codes.append("INSTRUMENT_METADATA_STALE")
        if obs.slippage_observation.source == "UNAVAILABLE":
            codes.append("SLIPPAGE_MODEL_UNAVAILABLE")
        if obs.funding_observation.source == "UNAVAILABLE":
            codes.append("FUNDING_UNAVAILABLE")
        strict_fatal = {"INSTRUMENT_METADATA_STALE", "SLIPPAGE_MODEL_UNAVAILABLE", "FUNDING_UNAVAILABLE"}
        kwargs = {f.name: getattr(obs, f.name) for f in fields(obs) if f.name not in ("observation_id", "observation_hash")}
        kwargs.update(reason_codes=tuple(dict.fromkeys(codes)),
                      source_quality="INVALID" if strict_fatal.intersection(codes) else obs.source_quality)
        return VenueEconomicObservation.build(**kwargs)


class BybitEconomicAdapter(PerpetualEconomicAdapter):
    adapter_id, broker, venue_id = "bybit_linear", "BYBIT", "BYBIT_LINEAR"


class BingXEconomicAdapter(PerpetualEconomicAdapter):
    adapter_id, broker, venue_id = "bingx_swap", "BINGX", "BINGX_SWAP"


_cache = weakref.WeakKeyDictionary()
_lock = threading.RLock()
FAILED_READ_BACKOFF_MS = 10_000


def collect_perpetual_raw(client, symbol, *, user_id=None, broker_account_id=None, environment=None):
    """Bounded per-client/account/symbol cache, sanitized reads through existing clients.
    Failed reads cached briefly too; no private response body or exception is persisted.
    """
    key = (user_id, broker_account_id, environment, symbol)
    with _lock:
        cache = _cache.setdefault(client, {})
        now = int(time.time() * 1000)
        cached = cache.get(key)
        if cached and 0 <= now - cached.captured_at < 2_000:
            return cached
        payload, reasons = {}, []
        # Metadata and fee observations have their own cache; never refresh timestamps on reuse.
        for name, ttl, call in (
            ("instrument", 3_600_000, lambda: client.get_instrument(symbol)),
            ("account_fee", 60_000, lambda: client.get_trading_fee_rates(symbol)),
        ):
            ck = (key, name)
            prior = cache.get(ck)
            # a failed read is retried after a short back-off, not held for the full TTL
            if prior and 0 <= now - prior[0] < (ttl if prior[1] is not None else FAILED_READ_BACKOFF_MS):
                stamp, value = prior
            else:
                try:
                    value = call()
                except Exception:
                    value = None
                stamp = int(time.time() * 1000)
                cache[ck] = (stamp, value)
            if name == "instrument":
                payload.update(instrument=value, metadata_as_of=stamp)
            elif value:
                payload[name] = {"maker": value.get("maker"), "taker": value.get("taker"), "as_of": stamp,
                    "user_id": user_id, "broker_account_id": broker_account_id, "environment": environment,
                    "venue_symbol": symbol}
        for name, call, keys in (
            ("book", lambda: client.get_orderbook(symbol, limit=50), ("bids", "asks", "time")),
            ("funding", lambda: client.get_funding(symbol), ("fundingRate", "nextFundingTime", "markPrice", "indexPrice",
                                                              "fundingIntervalHours")),
        ):
            try:
                data = call()
                payload[name] = {k: data.get(k) for k in keys}
                payload[name + "_as_of"] = int(time.time() * 1000)
            except Exception:
                reasons.append("COLLECTION_ERROR")
        result = VenueRawSnapshot(symbol, payload, int(time.time() * 1000), tuple(reasons))
        if len(cache) > 512:
            cache.clear()
        cache[key] = result
        return result
