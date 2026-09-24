"""Section 17/18 fixtures: recorded Binance Demo payloads + normalized Forex
and dated-futures broker payloads (test INPUT data; every observation, cost
and plan downstream is computed by the real pipeline)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from app.trading_intelligence.contracts.instrument import FUTURES, InstrumentKey, from_fx_pair, from_symbol_fallback
from app.trading_intelligence.venue.adapter import VenueEconomicRequest, VenueRawSnapshot
from app.trading_intelligence.venue.binance import BinanceUsdmEconomicAdapter
from app.trading_intelligence.venue.contract_suite import ContractCase
from app.trading_intelligence.venue.reference_adapters import DatedFuturesEconomicAdapter, ForexEconomicAdapter

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "binance_demo_recorded.json").read_text())
FAKE_SECRETS = ("fake-secret-value-9f8e7d", "fake-api-key-value-1a2b3c")

#: a Wednesday 10:00 New York (14:00 UTC) -- FX open, far from rollover
FX_OPEN_MS = int(datetime(2026, 9, 23, 14, 0, tzinfo=timezone.utc).timestamp() * 1000)
#: a Saturday -- FX closed
FX_CLOSED_MS = int(datetime(2026, 9, 26, 14, 0, tzinfo=timezone.utc).timestamp() * 1000)

#: a validated status registry for fixtures that must exercise the downstream
#: pipeline with Forex/futures adapters (the runtime registry keeps them UNVALIDATED)
FIXTURE_REGISTRY = {("forex_reference", "DEMO"): "SHADOW_VALIDATED",
                    ("dated_futures_reference", "DEMO"): "SHADOW_VALIDATED",
                    ("binance_usdm", "DEMO"): "DEMO_VALIDATED"}


# -- Binance (recorded real demo payloads) -----------------------------------------------
def binance_raw(symbol: str = "BTCUSDT", *, with_depth: bool = True, commission: Optional[dict] = None,
                premium: Optional[dict] = None) -> VenueRawSnapshot:
    rec = FIXTURE["symbols"][symbol]
    payloads = {"exchange_info_symbol": rec["exchange_info_symbol"], "exchange_info_as_of": FIXTURE["exchange_info_server_time"],
                "book_ticker": rec["book_ticker"], "premium_index": premium or rec["premium_index"],
                "funding_info": rec["funding_info"]}
    if with_depth:
        payloads["depth"] = rec["depth"]
    if commission is not None:
        payloads["commission_rate"] = commission
    captured = max(int(rec["book_ticker"]["time"]), int(rec["premium_index"]["time"]), int(rec["depth"]["T"]))
    return VenueRawSnapshot(venue_symbol=symbol, payloads=payloads, captured_at=captured)


def binance_key(symbol: str = "BTCUSDT") -> InstrumentKey:
    return from_symbol_fallback(venue="BinanceFuturesClient", venue_symbol=symbol)


def binance_request(symbol="BTCUSDT", *, raw=None, env="DEMO", account="acct-1", user="u1", bot="botA",
                    decision_time=None, health=None) -> VenueEconomicRequest:
    raw = raw or binance_raw(symbol)
    adapter = BinanceUsdmEconomicAdapter()
    t = decision_time if decision_time is not None else adapter.causal_decision_time(raw, raw.captured_at)
    return VenueEconomicRequest(instrument_key=binance_key(symbol), environment=env, decision_time=t, user_id=user,
                                broker_account_id=account, bot_instance_id=bot, run_id="run1", cycle_id="cyc1",
                                broker_health=health)


def binance_case(symbol="BTCUSDT") -> ContractCase:
    raw = binance_raw(symbol)
    info = FIXTURE["symbols"][symbol]["exchange_info_symbol"]
    flt = {f["filterType"]: f for f in info["filters"]}
    return ContractCase(
        adapter=BinanceUsdmEconomicAdapter(), raw=raw, request=binance_request(symbol, raw=raw),
        expected=dict(canonical_symbol=binance_key(symbol).canonical_symbol, venue_symbol=symbol, environment="DEMO",
                      tick_size=float(flt["PRICE_FILTER"]["tickSize"]), step_size=float(flt["LOT_SIZE"]["stepSize"]),
                      minimum_quantity=float(flt["LOT_SIZE"]["minQty"]),
                      minimum_notional=float(flt["MIN_NOTIONAL"]["notional"]), contract_multiplier=1.0,
                      funding_applicable=True, financing_applicable=False, carry_applicable=False, mark_index=True,
                      order_types=("LIMIT", "MARKET", "STOP_MARKET")),
        metadata_payload_key="exchange_info_symbol", forbidden_values=FAKE_SECRETS,
    )


# -- Forex (normalized broker payloads) ----------------------------------------------------
def fx_raw(symbol="EURUSD", *, t=FX_OPEN_MS, commission=None, financing="default", quote=None,
           with_depth=False) -> VenueRawSnapshot:
    base, q = symbol[:3], symbol[3:6]
    jpy = q == "JPY"
    px = 150.00 if jpy else 1.10000
    tick = 0.001 if jpy else 0.00001
    payloads = {
        "instrument": {"symbol": symbol, "base": base, "quote": q, "tick_size": tick, "pip_size": tick * 10,
                       "step_size": 1, "min_qty": 1, "lot_size": 100000, "as_of": t - 60_000},
        "quote": quote or {"bid": px, "ask": px + 8 * tick, "bid_qty": 5_000_000, "ask_qty": 5_000_000, "time": t - 500},
        "commission": commission if commission is not None else {"model": "SPREAD_ONLY"},
        "capabilities": {"order_types": ["MARKET", "LIMIT", "STOP"], "time_in_force": ["GTC", "IOC", "FOK", "GTD"],
                         "partial_close": True, "hedge_mode": False, "one_way_mode": True, "margin_modes": ["CROSS"]},
    }
    if financing == "default":
        payloads["financing"] = {"swap_long": -0.045, "swap_short": 0.012, "unit": "ANNUAL_RATE", "triple_weekday": 2,
                                 "as_of": t - 3_600_000}
    elif financing is not None:
        payloads["financing"] = financing
    if with_depth:
        payloads["depth"] = {"bids": [[px - i * tick, 2_000_000] for i in range(10)],
                             "asks": [[px + (8 + i) * tick, 2_000_000] for i in range(10)], "time": t - 400}
    return VenueRawSnapshot(venue_symbol=symbol, payloads=payloads, captured_at=t)


def fx_key(symbol="EURUSD") -> InstrumentKey:
    return from_fx_pair(venue="oanda", venue_symbol=symbol, base=symbol[:3], quote=symbol[3:6])


def fx_request(symbol="EURUSD", *, t=FX_OPEN_MS, env="DEMO", account="fx-acct", user="u1") -> VenueEconomicRequest:
    return VenueEconomicRequest(instrument_key=fx_key(symbol), environment=env, decision_time=t, user_id=user,
                                broker_account_id=account, bot_instance_id="fxbot", run_id="run1", cycle_id="cyc1")


def fx_case(symbol="EURUSD") -> ContractCase:
    raw = fx_raw(symbol, with_depth=True)
    return ContractCase(
        adapter=ForexEconomicAdapter(status_registry=FIXTURE_REGISTRY), raw=raw, request=fx_request(symbol),
        expected=dict(canonical_symbol=fx_key(symbol).canonical_symbol, venue_symbol=symbol, environment="DEMO",
                      tick_size=0.00001, step_size=1.0, minimum_quantity=1.0, minimum_notional=None,
                      contract_multiplier=1.0, funding_applicable=False, financing_applicable=True,
                      carry_applicable=False, order_types=("MARKET", "LIMIT")),
        forbidden_values=FAKE_SECRETS,
    )


# -- Dated futures (normalized broker payloads) ------------------------------------------------
FUT_EXPIRY_MS = FX_OPEN_MS + 60 * 86_400_000


def fut_raw(*, t=FX_OPEN_MS, session="OPEN", commission="default", spot=5000.0, fut_px=5025.0,
            expiry=FUT_EXPIRY_MS) -> VenueRawSnapshot:
    payloads = {
        "contract": {"symbol": "ESZ6", "root": "ES", "expiry_ms": expiry, "multiplier": 50, "tick_size": 0.25,
                     "tick_value": 12.5, "currency": "USD", "min_qty": 1, "step": 1, "as_of": t - 60_000},
        "quote": {"bid": fut_px - 0.125, "ask": fut_px + 0.125, "bid_qty": 40, "ask_qty": 35, "time": t - 300},
        "depth": {"bids": [[fut_px - 0.125 - 0.25 * i, 40] for i in range(10)],
                  "asks": [[fut_px + 0.125 + 0.25 * i, 35] for i in range(10)], "time": t - 300},
        "session_status": session,
        "capabilities": {"order_types": ["MARKET", "LIMIT", "STOP", "STOP_MARKET"], "time_in_force": ["DAY", "GTC", "IOC"],
                         "native_oco": True, "partial_close": True, "one_way_mode": True, "margin_modes": ["SPAN"]},
    }
    if commission == "default":
        payloads["commission"] = {"per_contract": 0.85, "exchange_fee": 1.28, "clearing_fee": 0.10, "currency": "USD"}
    elif commission is not None:
        payloads["commission"] = commission
    if spot is not None:
        payloads["reference_spot"] = {"price": spot, "time": t - 1_000}
    return VenueRawSnapshot(venue_symbol="ESZ6", payloads=payloads, captured_at=t)


def fut_key() -> InstrumentKey:
    return InstrumentKey(asset_class=FUTURES, base_asset="ES", quote_asset="USD", settlement_asset="USD",
                         contract_type="FUTURE", canonical_symbol="ES/USD:FUTURE", venue="ibkr", venue_symbol="ESZ6",
                         contract_multiplier=50.0)


def fut_request(*, t=FX_OPEN_MS, env="DEMO", account="fut-acct", user="u1") -> VenueEconomicRequest:
    return VenueEconomicRequest(instrument_key=fut_key(), environment=env, decision_time=t, user_id=user,
                                broker_account_id=account, bot_instance_id="futbot", run_id="run1", cycle_id="cyc1")


def fut_case() -> ContractCase:
    return ContractCase(
        adapter=DatedFuturesEconomicAdapter(status_registry=FIXTURE_REGISTRY), raw=fut_raw(), request=fut_request(),
        expected=dict(canonical_symbol="ES/USD:FUTURE", venue_symbol="ESZ6", environment="DEMO", tick_size=0.25,
                      step_size=1.0, minimum_quantity=1.0, minimum_notional=None, contract_multiplier=50.0,
                      funding_applicable=False, financing_applicable=False, carry_applicable=True,
                      order_types=("MARKET", "LIMIT", "STOP_MARKET")),
        metadata_payload_key="contract", forbidden_values=FAKE_SECRETS,
    )
