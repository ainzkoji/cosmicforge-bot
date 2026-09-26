"""Canonical broker instrument discovery (Phase 3A).

Every broker's instrument list is fetched from its own discovery API and
normalised into ``DiscoveredInstrument``. Nothing here holds a list of
tradable symbols: the universe is whatever the venue publishes.

Identity keeps venue observations separate:

* ``asset_class``      CRYPTO | FX | COMMODITIES | STOCK | INDEX | OTHER
* ``product_type``     PERPETUAL | FX_PERPETUAL | TRADFI_PERPETUAL | DELIVERY | SPOT | OTHER
* ``canonical_symbol`` venue-independent: ``BTC/USDT:PERP`` (the existing
                       app.universe.identity format), ``EUR/USD:FX_PERPETUAL``
* ``venue`` + ``venue_symbol`` what the order is sent with

Classification source is recorded. Venue metadata (Binance
``underlyingType`` / ``contractType``, Bybit ``symbolType``, the BingX
``NCFX``/``NCSK``/``NCCO``/``NCSI`` TradFi symbol namespaces of its official
swap contract list) wins; the symbol-shape fallback only recognises ISO-4217
fiat bases (FX) and precious metal codes (commodities) and says so
(``classification_source="SYMBOL_HEURISTIC"``). A venue product type this
module does not know is OTHER -- never guessed into a tradable class (Bybit
ETF perpetuals and BingX NC* TradFi contracts were once defaulted into CRYPTO).

FX identity is the currency PAIR, never the venue's coin label: Bybit lists
``EURUSDUSDT`` with ``baseCoin="EURUSD"``; its canonical symbol is
``EUR/USD:FX_PERPETUAL`` (economic base EUR, quote USD, settlement USDT).

Unknown numeric metadata stays ``None`` (never 0).
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from app.universe.identity import FUTURE as _ID_FUTURE
from app.universe.identity import PERP as _ID_PERP
from app.universe.identity import canonical_instrument

CRYPTO, FX, COMMODITIES, STOCK, INDEX, OTHER = "CRYPTO", "FX", "COMMODITIES", "STOCK", "INDEX", "OTHER"
PERPETUAL, FX_PERPETUAL, TRADFI_PERPETUAL, DELIVERY, SPOT, OTHER_PRODUCT = (
    "PERPETUAL", "FX_PERPETUAL", "TRADFI_PERPETUAL", "DELIVERY", "SPOT", "OTHER")

#: ISO-4217 currencies that trade as FX pairs (a symbol-shape fallback only).
FIAT = frozenset({
    "EUR", "USD", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD", "SEK", "NOK", "DKK", "PLN", "HUF", "CZK", "TRY",
    "ZAR", "MXN", "SGD", "HKD", "CNH", "KRW", "INR", "BRL", "ILS", "THB", "IDR", "PHP", "MYR", "TWD", "RUB",
})
STABLE_USD = frozenset({"USDT", "USDC", "FDUSD", "BUSD", "USD1", "TUSD", "DAI"})
METALS = frozenset({"XAU", "XAG", "XPT", "XPD", "GOLD", "SILVER"})

_UNDERLYING_TYPE = {  # venue metadata -> asset class
    # Binance underlyingType; Bybit symbolType ("innovation"/"adventure" are crypto listing zones)
    "COIN": CRYPTO, "CRYPTO": CRYPTO, "PREMARKET": CRYPTO, "INNOVATION": CRYPTO, "ADVENTURE": CRYPTO,
    "COMMODITY": COMMODITIES, "COMMODITIES": COMMODITIES, "METAL": COMMODITIES,
    "EQUITY": STOCK, "STOCK": STOCK, "STOCKS": STOCK, "XSTOCKS": STOCK, "ETF": STOCK,
    "HK_EQUITY": STOCK, "KR_EQUITY": STOCK, "CN_EQUITY": STOCK, "US_EQUITY": STOCK,
    "INDEX": INDEX, "INDICES": INDEX,
    "FX": FX, "FOREX": FX, "CURRENCY": FX,
}

#: BingX lists TradFi perpetuals in the same official swap contract API under
#: namespaced symbols: NCFX<BASE>2<QUOTE> (FX), NCSK<TICKER>2USD (stocks),
#: NCCO<NAME>2USD (commodities), NCSI<INDEX>2USD (indices).
_BINGX_NAMESPACE = {"NCFX": FX, "NCSK": STOCK, "NCCO": COMMODITIES, "NCSI": INDEX}
_COMMODITY_ALIASES = {"GOLD": "XAU", "SILVER": "XAG", "PLATINUM": "XPT", "PALLADIUM": "XPD"}


def fx_legs(base: str, quote: str) -> Tuple[str, str]:
    """Economic (base, quote) of an FX instrument.

    ``EURUSD`` + ``USDT`` -> (EUR, USD); ``GBP`` + ``USDT`` -> (GBP, USD).
    """
    b, q = str(base or "").upper(), str(quote or "").upper()
    if len(b) == 6 and b[:3] in FIAT and b[3:] in FIAT:
        return b[:3], b[3:]
    return b, ("USD" if q in STABLE_USD else q)


def bingx_namespace(asset: str) -> Optional[Tuple[str, str, str]]:
    """(asset_class, economic base, economic quote) for a BingX NC* symbol, else None."""
    import re

    a = str(asset or "").upper()
    m = re.fullmatch(r"(NC[A-Z]{2})(.+)2([A-Z]{3})", a)
    if not m:
        # observed variants without the "2" separator: NCSKTMFUSDT, NCCOXAGJPYUSD
        m = re.fullmatch(r"(NC[A-Z]{2})(.+?)(USDT|USD)", a)
        if not m or m.group(1) not in _BINGX_NAMESPACE:
            return None
    ac = _BINGX_NAMESPACE.get(m.group(1), OTHER)
    base = m.group(2)
    if m.group(3) == "USDT":
        return ac, base, "USD"
    if ac == COMMODITIES:
        base = _COMMODITY_ALIASES.get(base, base)
    return ac, base, m.group(3)


def _pos(v: Any) -> Optional[float]:
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def _int(v: Any) -> Optional[int]:
    try:
        return int(v) if v not in (None, "", "0", 0) else None
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class DiscoveredInstrument:
    venue: str
    venue_symbol: str
    asset_class: str
    product_type: str
    canonical_symbol: str
    base_currency: str
    quote_currency: str
    settlement_asset: str
    contract_type: str               # PERPETUAL | FUTURE | SPOT
    status: str                      # venue status, verbatim
    api_tradable: bool               # status says new orders are accepted via API
    tick_size: Optional[float]
    qty_step: Optional[float]
    min_qty: Optional[float]
    max_qty: Optional[float]
    min_notional: Optional[float]
    max_leverage: Optional[float]
    contract_multiplier: float = 1.0
    listed_at_ms: Optional[int] = None
    expiry_ms: Optional[int] = None
    funding_interval_minutes: Optional[int] = None
    margin_modes: Tuple[str, ...] = ()
    order_types: Tuple[str, ...] = ()
    session_restricted: Optional[bool] = None   # True = not 24/7 (TradFi sessions)
    classification_source: str = "VENUE_METADATA"
    venue_metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> Tuple[str, str]:
        return (self.venue, self.venue_symbol)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["margin_modes"], d["order_types"] = list(self.margin_modes), list(self.order_types)
        d["venue_metadata"] = dict(self.venue_metadata)
        return d

    def to_instrument_key(self):
        """CATI InstrumentKey (preserves CATI's own identity contract)."""
        from app.trading_intelligence.contracts import instrument as ik

        if self.asset_class == FX:
            return ik.from_fx_pair(venue=self.venue, venue_symbol=self.venue_symbol, base=self.base_currency,
                                   quote=self.quote_currency, contract_type=ik.PERPETUAL if self.contract_type == PERPETUAL else ik.SPOT)
        if self.asset_class == CRYPTO:
            kind = _ID_PERP if self.contract_type == PERPETUAL else _ID_FUTURE
            return ik.from_canonical(canonical=canonical_instrument(self.base_currency, self.quote_currency, kind),
                                     venue=self.venue, venue_symbol=self.venue_symbol, asset_class=ik.CRYPTO,
                                     settlement_asset=self.settlement_asset)
        return ik.instrument_key_for(venue=self.venue, venue_symbol=self.venue_symbol, asset_class=ik.FUTURES)

    def to_instrument_spec(self, broker_id: str):
        """The existing unified-trading InstrumentSpec (execution/sizing path)."""
        from app.models.unified_trading import AssetClass, InstrumentSpec

        ac = {CRYPTO: AssetClass.CRYPTO_PERP, FX: AssetClass.FOREX_PERP}.get(self.asset_class, AssetClass.TRADFI_PERP)
        if self.contract_type == SPOT:
            ac = AssetClass.CRYPTO_SPOT if self.asset_class == CRYPTO else AssetClass.FOREX_SPOT
        return InstrumentSpec(
            symbol_canonical=self.venue_symbol, symbol_exchange=self.venue_symbol, asset_class=ac,
            base_currency=self.base_currency, quote_currency=self.quote_currency,
            margin_currency=self.settlement_asset, settlement_currency=self.settlement_asset,
            contract_size=Decimal(str(self.contract_multiplier)),
            tick_size=Decimal(str(self.tick_size)) if self.tick_size else Decimal("0"),
            step_size=Decimal(str(self.qty_step)) if self.qty_step else Decimal("0"),
            min_qty=Decimal(str(self.min_qty)) if self.min_qty else Decimal("0"),
            min_notional=Decimal(str(self.min_notional)) if self.min_notional else None,
            price_precision=_decimals(self.tick_size), qty_precision=_decimals(self.qty_step),
            max_leverage=Decimal(str(self.max_leverage)) if self.max_leverage else Decimal("1"),
            supports_per_order_leverage=False,
        )


def _decimals(step: Optional[float]) -> int:
    if not step:
        return 8
    try:
        return max(0, -Decimal(str(step)).normalize().as_tuple().exponent)
    except (InvalidOperation, ValueError):
        return 8


def classify(base: str, quote: str, *, underlying_type: Optional[str] = None,
             contract_type: str = PERPETUAL) -> Tuple[str, str, str, str, str]:
    """(asset_class, product_type, canonical_symbol, economic_quote, source)."""
    b, q = str(base or "").upper(), str(quote or "").upper()
    if underlying_type:
        ac = _UNDERLYING_TYPE.get(str(underlying_type).upper())
        if ac:
            return _shape(ac, b, q, contract_type) + ("VENUE_METADATA",)
        # a venue product type this module does not know is NOT defaulted into crypto
        return _shape(OTHER, b, q, contract_type) + ("VENUE_METADATA_UNMAPPED",)
    if (b in FIAT and (q in FIAT or q in STABLE_USD)) or (len(b) == 6 and b[:3] in FIAT and b[3:] in FIAT):
        return _shape(FX, b, q, contract_type) + ("SYMBOL_HEURISTIC",)
    if b in METALS:
        return _shape(COMMODITIES, b, q, contract_type) + ("SYMBOL_HEURISTIC",)
    return _shape(CRYPTO, b, q, contract_type) + (("VENUE_METADATA" if underlying_type else "DEFAULT_CRYPTO"),)


def _shape(ac: str, b: str, q: str, contract_type: str) -> Tuple[str, str, str, str]:
    if ac == CRYPTO:
        kind = _ID_PERP if contract_type == PERPETUAL else _ID_FUTURE
        product = PERPETUAL if contract_type == PERPETUAL else (SPOT if contract_type == SPOT else DELIVERY)
        return CRYPTO, product, canonical_instrument(b, q, kind).canonical_id, q
    if ac == FX:
        # a USDT-quoted FX perpetual is a USD-pair proxy; a 6-letter pair label is split into its legs
        b, econ_q = fx_legs(b, q)
        product = FX_PERPETUAL if contract_type == PERPETUAL else (SPOT if contract_type == SPOT else DELIVERY)
        return FX, product, f"{b}/{econ_q}:{product}", econ_q
    econ_q = "USD" if q in STABLE_USD else q
    if ac == COMMODITIES:
        b = _COMMODITY_ALIASES.get(b, b)
    product = TRADFI_PERPETUAL if contract_type == PERPETUAL else DELIVERY
    return ac, product, f"{b}/{econ_q}:{product}", econ_q


# ── Venue parsers ────────────────────────────────────────────────────────────

def parse_binance_symbol(s: Mapping[str, Any], *, venue: str = "binance_usdm") -> Optional[DiscoveredInstrument]:
    sym = str(s.get("symbol") or "").upper()
    if not sym:
        return None
    ct = str(s.get("contractType") or "").upper()
    contract = PERPETUAL if ct in ("PERPETUAL", "TRADIFI_PERPETUAL") else ("FUTURE" if ct else "FUTURE")
    underlying = s.get("underlyingType")
    ac, product, canon, econ_q, source = classify(s.get("baseAsset", ""), s.get("quoteAsset", ""),
                                                  underlying_type=underlying, contract_type=contract)
    if ct == "TRADIFI_PERPETUAL" and ac == CRYPTO:
        ac, product, canon, source = OTHER, TRADFI_PERPETUAL, f"{s.get('baseAsset','')}/{s.get('quoteAsset','')}:TRADFI_PERPETUAL", "VENUE_METADATA"
    f = {x.get("filterType"): x for x in s.get("filters", []) or []}
    lot, price, notional = f.get("LOT_SIZE", {}), f.get("PRICE_FILTER", {}), f.get("MIN_NOTIONAL", {})
    status = str(s.get("status") or "")
    base_ccy = str(s.get("baseAsset", "")).upper()
    if ac == FX:
        base_ccy, econ_q = fx_legs(base_ccy, s.get("quoteAsset", ""))
    return DiscoveredInstrument(
        venue=venue, venue_symbol=sym, asset_class=ac, product_type=product,
        canonical_symbol=canon, base_currency=base_ccy, quote_currency=econ_q,
        settlement_asset=str(s.get("marginAsset") or s.get("quoteAsset") or "").upper(), contract_type=contract,
        status=status, api_tradable=status.upper() == "TRADING",
        tick_size=_pos(price.get("tickSize")), qty_step=_pos(lot.get("stepSize")), min_qty=_pos(lot.get("minQty")),
        max_qty=_pos(lot.get("maxQty")), min_notional=_pos(notional.get("notional") or notional.get("minNotional")),
        max_leverage=None,  # per-bracket (leverageBracket endpoint), not published in exchangeInfo
        listed_at_ms=_int(s.get("onboardDate")),
        expiry_ms=_int(s.get("deliveryDate")) if contract != PERPETUAL else None,
        order_types=tuple(s.get("orderTypes") or ()), session_restricted=(True if ac in (STOCK, INDEX) else None),
        classification_source=source,
        venue_metadata={k: s.get(k) for k in ("contractType", "underlyingType", "underlyingSubType", "marginAsset")
                        if s.get(k) is not None},
    )


def parse_bybit_instrument(r: Mapping[str, Any], *, category: str = "linear",
                           venue: str = "bybit_linear") -> Optional[DiscoveredInstrument]:
    sym = str(r.get("symbol") or "").upper()
    if not sym:
        return None
    ct = str(r.get("contractType") or "").upper()  # LinearPerpetual | LinearFutures | InversePerpetual
    contract = PERPETUAL if "PERPETUAL" in ct else ("SPOT" if category == "spot" else "FUTURE")
    ac, product, canon, econ_q, source = classify(r.get("baseCoin", ""), r.get("quoteCoin", ""),
                                                  underlying_type=r.get("symbolType") or None, contract_type=contract)
    lot, price, lev = r.get("lotSizeFilter") or {}, r.get("priceFilter") or {}, r.get("leverageFilter") or {}
    status = str(r.get("status") or "")
    base_ccy = str(r.get("baseCoin", "")).upper()
    if ac == FX:
        base_ccy, econ_q = fx_legs(base_ccy, r.get("quoteCoin", ""))
    return DiscoveredInstrument(
        venue=venue, venue_symbol=sym, asset_class=ac, product_type=product, canonical_symbol=canon,
        base_currency=base_ccy, quote_currency=econ_q,
        settlement_asset=str(r.get("settleCoin") or r.get("quoteCoin") or "").upper(), contract_type=contract,
        status=status, api_tradable=status.lower() == "trading",
        tick_size=_pos(price.get("tickSize")), qty_step=_pos(lot.get("qtyStep") or lot.get("basePrecision")),
        min_qty=_pos(lot.get("minOrderQty")), max_qty=_pos(lot.get("maxOrderQty")),
        min_notional=_pos(lot.get("minNotionalValue")), max_leverage=_pos(lev.get("maxLeverage")),
        listed_at_ms=_int(r.get("launchTime")), expiry_ms=_int(r.get("deliveryTime")) if contract != PERPETUAL else None,
        funding_interval_minutes=_int(r.get("fundingInterval")),
        session_restricted=(True if ac in (STOCK, INDEX) else None), classification_source=source,
        venue_metadata={**{k: r.get(k) for k in ("contractType", "symbolType", "settleCoin", "copyTrading", "isPreListing")
                           if r.get(k) not in (None, "")},
                        # exact venue precision strings (never re-rendered from floats)
                        "raw_filters": {"tickSize": price.get("tickSize"), "qtyStep": lot.get("qtyStep"),
                                        "minOrderQty": lot.get("minOrderQty"), "maxOrderQty": lot.get("maxOrderQty"),
                                        "minNotionalValue": lot.get("minNotionalValue")}},
    )


def parse_bingx_contract(c: Mapping[str, Any], *, venue: str = "bingx_swap") -> Optional[DiscoveredInstrument]:
    raw = str(c.get("symbol") or "").upper()
    if not raw:
        return None
    base, _, quote = raw.partition("-")
    quote = str(c.get("currency") or quote or "USDT").upper()
    base = str(c.get("asset") or base).upper()
    ns = bingx_namespace(base)
    if ns is not None:
        ns_class, ns_base, ns_quote = ns
        ac, product, canon, econ_q, _src = classify(ns_base, ns_quote, underlying_type=ns_class,
                                                    contract_type=PERPETUAL)
        source = "VENUE_SYMBOL_NAMESPACE" if ns_class != OTHER else "VENUE_SYMBOL_NAMESPACE_UNMAPPED"
        base = fx_legs(ns_base, ns_quote)[0] if ac == FX else canon.split("/")[0]
    else:
        ac, product, canon, econ_q, source = classify(base, quote, contract_type=PERPETUAL)
    status = c.get("status")
    api_open = c.get("apiStateOpen")
    tradable = (str(status) == "1") and (api_open in (None, True, "true", "True"))
    price_prec = _int(c.get("pricePrecision"))
    qty_prec = _int(c.get("quantityPrecision"))
    return DiscoveredInstrument(
        venue=venue, venue_symbol=raw, asset_class=ac, product_type=product, canonical_symbol=canon,
        base_currency=base, quote_currency=econ_q, settlement_asset=quote, contract_type=PERPETUAL,
        status=str(status), api_tradable=tradable,
        tick_size=(10 ** -price_prec) if price_prec is not None else None,
        qty_step=_pos(c.get("size")) or ((10 ** -qty_prec) if qty_prec is not None else None),
        min_qty=_pos(c.get("tradeMinQuantity")), max_qty=None, min_notional=_pos(c.get("tradeMinUSDT")),
        max_leverage=None, listed_at_ms=_int(c.get("launchTime")),
        session_restricted=(True if ns is not None and ac in (FX, STOCK, INDEX, COMMODITIES) else None),
        classification_source=source,
        venue_metadata={k: c.get(k) for k in ("apiStateOpen", "apiStateClose", "status", "displayName") if c.get(k) is not None},
    )


# ── Persistent catalog ───────────────────────────────────────────────────────

class InstrumentCatalog:
    """``venue_instruments``: last discovered state of every venue instrument.

    Venue-global (no account data). A symbol that disappears from discovery
    is marked ``delisted_at`` -- never deleted -- so research manifests keep
    their listing/delisting boundaries.
    """

    def __init__(self, db: Any):
        self.db = db

    def upsert(self, venue: str, environment: str, instruments: Iterable[DiscoveredInstrument], now_ms: int) -> Dict[str, int]:
        """Idempotent: re-recording identical metadata changes only ``last_seen_ms``. ``first_seen_ms``
        is never rewritten; a relisted symbol clears ``delisted_at_ms``; a metadata change is stamped."""
        from app.exchange.canonical_registry import metadata_version

        instruments = list(instruments)
        if not instruments:
            # an empty discovery is a venue/transport failure, never "everything was delisted"
            raise ValueError(f"{venue}/{environment}: empty discovery refused; catalog left unchanged")
        seen = set()
        counts = {"discovered": 0, "new": 0, "delisted": 0, "relisted": 0, "metadata_changed": 0}
        with self.db.connect() as conn:
            known = {r[0]: (r[1], r[2]) for r in conn.execute(
                "SELECT venue_symbol, delisted_at_ms, metadata_hash FROM venue_instruments WHERE venue=? AND "
                "environment=?", (venue, environment)).fetchall()}
            existing = {s for s, (delisted, _h) in known.items() if delisted is None}
            for ins in instruments:
                if ins.venue_symbol in seen:
                    continue  # a venue listing the same symbol twice in one response is recorded once
                seen.add(ins.venue_symbol)
                counts["discovered"] += 1
                mhash = metadata_version(ins)
                prev = known.get(ins.venue_symbol)
                if prev is None:
                    counts["new"] += 1
                elif prev[0] is not None:
                    counts["relisted"] += 1
                changed = prev is not None and prev[1] is not None and prev[1] != mhash
                counts["metadata_changed"] += int(changed)
                conn.execute(
                    """INSERT INTO venue_instruments (venue, environment, venue_symbol, asset_class, product_type,
                       canonical_symbol, status, api_tradable, payload_json, first_seen_ms, last_seen_ms, delisted_at_ms,
                       metadata_hash, metadata_changed_ms)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL,?,NULL)
                       ON CONFLICT(venue, environment, venue_symbol) DO UPDATE SET asset_class=excluded.asset_class,
                       product_type=excluded.product_type, canonical_symbol=excluded.canonical_symbol,
                       status=excluded.status, api_tradable=excluded.api_tradable, payload_json=excluded.payload_json,
                       last_seen_ms=excluded.last_seen_ms, delisted_at_ms=NULL, metadata_hash=excluded.metadata_hash,
                       metadata_changed_ms=CASE WHEN venue_instruments.metadata_hash IS NOT NULL AND
                           venue_instruments.metadata_hash != excluded.metadata_hash THEN ?
                           ELSE venue_instruments.metadata_changed_ms END""",
                    (venue, environment, ins.venue_symbol, ins.asset_class, ins.product_type, ins.canonical_symbol,
                     ins.status, int(ins.api_tradable), json.dumps(ins.to_dict(), default=str), now_ms, now_ms,
                     mhash, now_ms))
            for gone in sorted(existing - seen):
                conn.execute("UPDATE venue_instruments SET delisted_at_ms=?, api_tradable=0 WHERE venue=? AND "
                             "environment=? AND venue_symbol=?", (now_ms, venue, environment, gone))
                counts["delisted"] += 1
        return counts

    def active_count(self, venue: str, environment: str) -> int:
        with self.db.connect() as conn:
            r = conn.execute("SELECT COUNT(*) FROM venue_instruments WHERE venue=? AND environment=? AND "
                             "delisted_at_ms IS NULL", (venue, environment)).fetchone()
        return int(r[0] or 0)

    def last_synced_ms(self, venue: str, environment: str) -> Optional[int]:
        """When this venue/environment catalog was last refreshed (None = never)."""
        with self.db.connect() as conn:
            r = conn.execute("SELECT MAX(last_seen_ms) FROM venue_instruments WHERE venue=? AND environment=?",
                             (venue, environment)).fetchone()
        return int(r[0]) if r and r[0] is not None else None

    def record(self, venue: str, environment: str, venue_symbol: str) -> Optional[Dict[str, Any]]:
        """Historical identity of one venue instrument -- delisted ones included (research lineage)."""
        with self.db.connect() as conn:
            r = conn.execute("SELECT payload_json, first_seen_ms, last_seen_ms, delisted_at_ms, metadata_hash, "
                             "metadata_changed_ms FROM venue_instruments WHERE venue=? AND environment=? AND "
                             "venue_symbol=?", (venue, environment, str(venue_symbol).upper())).fetchone()
        if r is None:
            return None
        return {"instrument": from_dict(json.loads(r[0])), "first_seen_ms": r[1], "last_seen_ms": r[2],
                "delisted_at_ms": r[3], "metadata_hash": r[4], "metadata_changed_ms": r[5],
                "state": "DELISTED" if r[3] is not None else "LISTED"}

    def list(self, venue: str, environment: str, *, asset_class: Optional[str] = None,
             tradable_only: bool = True, include_delisted: bool = False) -> List[DiscoveredInstrument]:
        sql = "SELECT payload_json FROM venue_instruments WHERE venue=? AND environment=?"
        if not include_delisted:
            sql += " AND delisted_at_ms IS NULL"
        args: List[Any] = [venue, environment]
        if asset_class:
            sql += " AND asset_class=?"
            args.append(asset_class)
        if tradable_only:
            sql += " AND api_tradable=1"
        with self.db.connect() as conn:
            rows = conn.execute(sql + " ORDER BY venue_symbol", args).fetchall()
        return [from_dict(json.loads(r[0])) for r in rows]


def from_dict(d: Mapping[str, Any]) -> DiscoveredInstrument:
    d = dict(d)
    d["margin_modes"] = tuple(d.get("margin_modes") or ())
    d["order_types"] = tuple(d.get("order_types") or ())
    return DiscoveredInstrument(**d)


__all__ = [
    "CRYPTO", "FX", "COMMODITIES", "STOCK", "INDEX", "OTHER", "PERPETUAL", "FX_PERPETUAL", "TRADFI_PERPETUAL",
    "DELIVERY", "SPOT", "DiscoveredInstrument", "InstrumentCatalog", "bingx_namespace", "classify",
    "collect_cursor_pages", "from_dict",
    "fx_legs",
    "parse_binance_symbol", "parse_bingx_contract", "parse_bybit_instrument",
]


# ── Account-level execution eligibility (Phase 3E) ───────────────────────────

#: classification sources that come from the venue itself (metadata or its documented symbol namespace)
VENUE_EVIDENCED_SOURCES = frozenset({"VENUE_METADATA", "VENUE_SYMBOL_NAMESPACE"})

_PRODUCT_CAPABILITY = {CRYPTO: "crypto_perpetuals", FX: "fx_perpetuals", COMMODITIES: "tradfi", STOCK: "tradfi",
                       INDEX: "tradfi"}


def execution_eligibility(ins: DiscoveredInstrument, *, broker: str, environment: str,
                          permissions: Optional[Mapping[str, Any]] = None) -> Tuple[bool, Tuple[str, ...]]:
    """Can THIS account execute THIS discovered instrument now?

    Market availability (the venue lists it) and API execution (this
    platform's adapter + this key may trade it) are separate facts: an FX
    instrument BingX lists is market-available but API-execution
    VENUE_API_NOT_SUPPORTED.
    """
    from shared_lib.broker.capabilities import Capability, execution_readiness

    reasons = []
    if not ins.api_tradable:
        reasons.append("INSTRUMENT_NOT_API_TRADABLE")
        # BingX publishes whether the API accepts NEW orders per contract: a listed contract whose
        # apiStateOpen is false is market-known but not API-executable.
        if str((ins.venue_metadata or {}).get("apiStateOpen", "")).lower() == "false":
            reasons.append("VENUE_API_NOT_SUPPORTED")
    if ins.asset_class != CRYPTO and ins.classification_source not in VENUE_EVIDENCED_SOURCES:
        # a symbol-shape guess never authorizes a TradFi order
        reasons.append("CLASSIFICATION_NOT_VENUE_EVIDENCED")
    if ins.contract_type != PERPETUAL:
        reasons.append("CONTRACT_TYPE_NOT_SUPPORTED")
    cap = _PRODUCT_CAPABILITY.get(ins.asset_class)
    if cap is None:
        reasons.append("ASSET_CLASS_NOT_SUPPORTED")
    else:
        r = execution_readiness(broker, environment, permissions=permissions, product=Capability(cap))
        if not r.permitted:
            from shared_lib.broker.capabilities import CapabilityState, declared_profile

            entry = declared_profile(broker).entry(Capability(cap))
            hard = entry.state in (CapabilityState.UNSUPPORTED, CapabilityState.VENUE_API_UNAVAILABLE)
            # a product the platform/venue cannot trade names ITS reason (e.g.
            # VENUE_API_NOT_SUPPORTED); otherwise the account-level reason.
            reasons.append(entry.reason_code if hard and entry.reason_code else (r.reason_code or "NOT_PERMITTED"))
    return (not reasons), tuple(reasons)


def collect_cursor_pages(fetch_page: Any, *, source: str, max_pages: int = 50) -> List[Any]:
    """Every row of a cursor-paginated venue listing, or an exception -- never a truncated list.

    ``fetch_page(cursor)`` -> ``(rows, next_cursor)`` (first call with ``None``). A repeated cursor or a
    chain that does not end within ``max_pages`` raises: a partial universe would otherwise be recorded
    as delistings of every instrument on the missing pages."""
    out: List[Any] = []
    cursor, seen = None, set()
    for _ in range(max_pages):
        rows, cursor = fetch_page(cursor)
        out.extend(rows or [])
        cursor = cursor or None
        if not cursor:
            return out
        if cursor in seen:
            raise RuntimeError(f"{source}: repeated pagination cursor; discovery incomplete")
        seen.add(cursor)
    raise RuntimeError(f"{source}: pagination did not terminate; discovery incomplete")


#: A refresh that would delist more than this share of a catalog of at least
#: ``_MASS_DELIST_MIN_KNOWN`` live instruments is treated as a partial/failed
#: venue response, not as a mass delisting (venues delist a few symbols at a time).
MASS_DELIST_MAX_FRACTION = 0.5
_MASS_DELIST_MIN_KNOWN = 20
REASON_DISCOVERY_SUSPECT_PARTIAL = "DISCOVERY_SUSPECT_PARTIAL"


def sync_instruments(client: Any, *, catalog: "InstrumentCatalog", venue: str, environment: str,
                     now_ms: int) -> Dict[str, int]:
    """Discover from the broker and persist (Phase 3A). Raises on a discovery
    failure so a partial list is never recorded as a delisting."""
    instruments = client.discover_instruments()
    if not instruments:
        raise RuntimeError(f"{venue}: discovery returned no instruments; catalog left unchanged")
    known = catalog.active_count(venue, environment)
    if known >= _MASS_DELIST_MIN_KNOWN:
        returned = {i.venue_symbol for i in instruments}
        if len(returned) < known * (1.0 - MASS_DELIST_MAX_FRACTION):
            raise RuntimeError(f"{REASON_DISCOVERY_SUSPECT_PARTIAL}: {venue}/{environment} returned {len(returned)} "
                               f"of {known} known instruments; catalog left unchanged")
    return catalog.upsert(venue, environment, instruments, now_ms)
