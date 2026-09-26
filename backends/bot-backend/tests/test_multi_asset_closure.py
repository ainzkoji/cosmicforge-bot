"""Multi-asset closure: discovery on recorded LIVE venue shapes, canonical registry,
reference-vs-execution mapping, account capability engine, auto-activation,
frozen universes, FX reference ingestion helpers and certification wiring.

Venue payloads below are verbatim shapes recorded from the public official
APIs on 2026-09-26 (Bybit V5 instruments-info, BingX swap contracts); no key.
"""
from __future__ import annotations

import json
import lzma
import struct
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.exchange.instruments import (COMMODITIES, CRYPTO, FX, INDEX, OTHER, STOCK, bingx_namespace,
                                      execution_eligibility, fx_legs, parse_binance_symbol, parse_bingx_contract,
                                      parse_bybit_instrument)

# ── recorded venue shapes ──────────────────────────────────────────────────

def _bybit(symbol, base, symbol_type, *, status="Trading", quote="USDT", launch="1788858596000"):
    return {"symbol": symbol, "baseCoin": base, "quoteCoin": quote, "settleCoin": quote, "symbolType": symbol_type,
            "contractType": "LinearPerpetual", "status": status, "launchTime": launch, "fundingInterval": 480,
            "lotSizeFilter": {"maxOrderQty": "356000.0", "minOrderQty": "0.1", "qtyStep": "0.1",
                              "minNotionalValue": "5"},
            "priceFilter": {"minPrice": "0.00001", "maxPrice": "199.99998", "tickSize": "0.00001"},
            "leverageFilter": {"maxLeverage": "100"}}


BYBIT_EURUSD = _bybit("EURUSDUSDT", "EURUSD", "forex")
BYBIT_USDJPY = _bybit("USDJPYUSDT", "USDJPY", "forex")
BYBIT_SPY_ETF = _bybit("SPYUSDT", "SPY", "ETF")
BYBIT_AAPL = _bybit("AAPLUSDT", "AAPL", "stock")
BYBIT_XAU = _bybit("XAUUSDT", "XAU", "commodity")
BYBIT_BTC = _bybit("BTCUSDT", "BTC", "", launch="1585526400000")
BYBIT_INNOVATION = _bybit("NEWCOINUSDT", "NEWCOIN", "innovation")


def _bingx(symbol, asset, *, status=1, api_open="true", display=""):
    return {"contractId": "1", "symbol": symbol, "size": "0.0001", "quantityPrecision": 4, "pricePrecision": 5,
            "currency": "USDT", "asset": asset, "status": status, "apiStateOpen": api_open, "apiStateClose": "true",
            "tradeMinQuantity": 0.0001, "tradeMinUSDT": 2, "launchTime": 1586275200000, "displayName": display}


BINGX_EURUSD = _bingx("NCFXEUR2USD-USDT", "NCFXEUR2USD", status=25, display="EURUSD-USDT")
BINGX_GOLD = _bingx("NCCOGOLD2USD-USDT", "NCCOGOLD2USD", display="GOLD(XAU)-USDT")
BINGX_TSLA = _bingx("NCSKTSLA2USD-USDT", "NCSKTSLA2USD", display="TSLA-USDT")
BINGX_NIKKEI = _bingx("NCSINIKKEI2252USD-USDT", "NCSINIKKEI2252USD", status=25, display="NIKKEI225-USDT")
BINGX_TMF = _bingx("NCSKTMFUSDT-USDT", "NCSKTMFUSDT", display="TMF-USDT")
BINGX_BTC = _bingx("BTC-USDT", "BTC", display="BTC-USDT")


# ── 1-7 classification / canonical mapping ─────────────────────────────────

def test_bybit_fx_perpetual_identity_is_the_currency_pair():
    eur, jpy = parse_bybit_instrument(BYBIT_EURUSD), parse_bybit_instrument(BYBIT_USDJPY)
    assert (eur.asset_class, eur.canonical_symbol, eur.base_currency, eur.quote_currency, eur.settlement_asset) == (
        FX, "EUR/USD:FX_PERPETUAL", "EUR", "USD", "USDT")
    assert (jpy.canonical_symbol, jpy.base_currency, jpy.quote_currency) == ("USD/JPY:FX_PERPETUAL", "USD", "JPY")
    assert eur.classification_source == "VENUE_METADATA" and eur.api_tradable
    assert eur.to_instrument_key().asset_class == "FX" and eur.to_instrument_key().base_asset == "EUR"
    assert fx_legs("EURUSD", "USDT") == ("EUR", "USD") and fx_legs("GBP", "USDC") == ("GBP", "USD")


def test_bybit_etf_and_stock_perpetuals_are_never_crypto():
    for payload in (BYBIT_SPY_ETF, BYBIT_AAPL):
        ins = parse_bybit_instrument(payload)
        assert ins.asset_class == STOCK and ins.product_type == "TRADFI_PERPETUAL" and ins.session_restricted
    assert parse_bybit_instrument(BYBIT_XAU).canonical_symbol == "XAU/USD:TRADFI_PERPETUAL"
    assert parse_bybit_instrument(BYBIT_BTC).canonical_symbol == "BTC/USDT:PERP"
    assert parse_bybit_instrument(BYBIT_INNOVATION).asset_class == CRYPTO
    # an unknown venue product type is OTHER, never defaulted into crypto
    assert parse_bybit_instrument(_bybit("ZZZUSDT", "ZZZ", "someNewType")).asset_class == OTHER


def test_bingx_tradfi_namespaces_are_classified_from_the_official_contract_list():
    eur, gold, tsla, nik, tmf, btc = (parse_bingx_contract(c) for c in (
        BINGX_EURUSD, BINGX_GOLD, BINGX_TSLA, BINGX_NIKKEI, BINGX_TMF, BINGX_BTC))
    assert (eur.asset_class, eur.canonical_symbol, eur.base_currency, eur.quote_currency) == (
        FX, "EUR/USD:FX_PERPETUAL", "EUR", "USD")
    assert (gold.asset_class, gold.canonical_symbol) == (COMMODITIES, "XAU/USD:TRADFI_PERPETUAL")
    assert (tsla.asset_class, nik.asset_class, nik.base_currency) == (STOCK, INDEX, "NIKKEI225")
    assert tmf.asset_class == STOCK and tmf.base_currency == "TMF"  # separator-less variant
    assert btc.asset_class == CRYPTO and btc.classification_source == "DEFAULT_CRYPTO"
    assert all(i.classification_source == "VENUE_SYMBOL_NAMESPACE" for i in (eur, gold, tsla, nik, tmf))
    assert not eur.api_tradable and eur.status == "25"  # status 25 (session closed) is not tradable
    assert bingx_namespace("BTC") is None and bingx_namespace("NCXXFOO2USD")[0] == OTHER


def test_binance_regional_equities_and_existing_crypto_are_unchanged():
    btc = parse_binance_symbol({"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "status": "TRADING",
                                "contractType": "PERPETUAL", "underlyingType": "COIN", "filters": []})
    assert (btc.asset_class, btc.canonical_symbol) == (CRYPTO, "BTC/USDT:PERP")  # backward compatible
    hk = parse_binance_symbol({"symbol": "0700USDT", "baseAsset": "0700", "quoteAsset": "USDT", "status": "TRADING",
                               "contractType": "TRADIFI_PERPETUAL", "underlyingType": "HK_EQUITY", "filters": []})
    assert hk.asset_class == STOCK
    brl = parse_binance_symbol({"symbol": "USDBRLUSDT", "baseAsset": "USDBRL", "quoteAsset": "USDT",
                                "status": "TRADING", "contractType": "TRADIFI_PERPETUAL", "underlyingType": "FX",
                                "filters": []})
    assert (brl.asset_class, brl.base_currency, brl.quote_currency) == (FX, "USD", "BRL")


def test_canonical_registry_maps_venues_and_reports_duplicates():
    from app.exchange.canonical_registry import build_registry

    by, bx = parse_bybit_instrument(BYBIT_EURUSD), parse_bingx_contract(BINGX_EURUSD)
    reg = build_registry([by, bx, parse_bybit_instrument(BYBIT_BTC), parse_bingx_contract(BINGX_BTC)])
    eur = reg.instruments["EUR/USD:FX_PERPETUAL"]
    assert eur.reference_market_id == "FX:EUR/USD"
    assert [(v.venue, v.venue_symbol, v.api_order_supported) for v in eur.venues] == [
        ("bingx_swap", "NCFXEUR2USD-USDT", False), ("bybit_linear", "EURUSDUSDT", True)]
    assert eur.status == "TRADING"
    assert reg.by_venue_symbol("bybit_linear", "BTCUSDT").canonical_instrument_id == "BTC/USDT:PERP"
    assert reg.summary()["listed_on_multiple_venues"] == 2 and reg.summary()["conflicts"] == 0
    # the same venue symbol discovered twice with a different classification is a conflict, never merged
    clash = parse_bybit_instrument(_bybit("EURUSDUSDT", "EURUSD", "stock"))
    reg2 = build_registry([by, clash])
    assert "bybit_linear:EURUSDUSDT" in reg2.conflicts and reg2.instruments == {}
    assert build_registry([bx, by]).registry_hash == build_registry([by, bx]).registry_hash  # deterministic


def test_bybit_discovery_paginates_live_shapes():
    from app.exchange.bybit.client import BybitClient

    pages = {None: {"list": [BYBIT_BTC, BYBIT_EURUSD], "nextPageCursor": "c2"},
             "c2": {"list": [BYBIT_SPY_ETF, BYBIT_XAU], "nextPageCursor": ""}}
    c = BybitClient("k", "s", base_url="https://x")
    calls = []

    def fake(method, path, payload=None):
        calls.append(path)
        return {"retCode": 0, "result": pages[(payload or {}).get("cursor")]}

    c._request_v5 = fake  # type: ignore[attr-defined]
    found = {i.venue_symbol: i for i in c.discover_instruments()}
    assert calls == ["/v5/market/instruments-info"] * 2
    assert set(found) == {"BTCUSDT", "EURUSDUSDT", "SPYUSDT", "XAUUSDT"}
    assert found["SPYUSDT"].asset_class == STOCK and found["EURUSDUSDT"].asset_class == FX


# ── 8-9, 30 execution eligibility / permissions ────────────────────────────

def test_tradfi_execution_needs_venue_evidence_api_state_and_trading_status():
    ok, why = execution_eligibility(parse_bingx_contract(BINGX_GOLD), broker="bingx", environment="demo")
    assert ok, why
    ok, why = execution_eligibility(parse_bingx_contract({**BINGX_GOLD, "apiStateOpen": "false"}),
                                    broker="bingx", environment="demo")
    assert not ok and "VENUE_API_NOT_SUPPORTED" in why
    ok, why = execution_eligibility(parse_bingx_contract(BINGX_EURUSD), broker="bingx", environment="demo")
    assert why == ("INSTRUMENT_NOT_API_TRADABLE",)
    ok, why = execution_eligibility(parse_bybit_instrument(BYBIT_EURUSD), broker="bybit", environment="live")
    assert why == ("BROKER_EXECUTION_UNVALIDATED_FOR_LIVE",)
    ok, why = execution_eligibility(parse_bybit_instrument(BYBIT_EURUSD), broker="bybit", environment="demo",
                                    permissions={"TRADE": False})
    assert not ok


def test_withdrawal_permission_is_never_required_and_refuses_the_key():
    from shared_lib.broker.capabilities import WITHDRAWALS_SUPPORTED_BY_PLATFORM, execution_readiness

    from app.activation.market import account_market_status

    assert WITHDRAWALS_SUPPORTED_BY_PLATFORM is False
    r = execution_readiness("bybit", "demo", permissions={"TRADE": True, "WITHDRAW": True})
    assert not r.permitted and r.reason_code == "API_KEY_WITHDRAW_PERMISSION_PRESENT"
    st = account_market_status(broker="bybit", environment="DEMO", permissions={"TRADE": True, "WITHDRAW": True},
                               instruments=[parse_bybit_instrument(BYBIT_BTC)], transfers_in_flight=0)
    assert st["withdrawal_permission_required"] is False and st["withdraw_permission_present"] is True
    assert st["capabilities"]["CRYPTO_EXECUTION"]["status"] == "ACCOUNT_NOT_ELIGIBLE"


# ── account capability engine / UI statuses ───────────────────────────────

PERMS = {"READ_ACCOUNT": True, "READ_POSITIONS": True, "READ_ORDERS": True, "TRADE": True,
         "INTERNAL_TRANSFER": True, "WITHDRAW": False}


def test_one_bybit_connection_exposes_crypto_fx_and_tradfi_dynamically():
    from app.activation.market import account_market_status

    ins = [parse_bybit_instrument(p) for p in (BYBIT_BTC, BYBIT_EURUSD, BYBIT_USDJPY, BYBIT_SPY_ETF, BYBIT_XAU)]
    st = account_market_status(broker="bybit", environment="DEMO", permissions=PERMS, instruments=ins,
                               transfers_in_flight=0, health={"quarantined": False}, account_mode="UNIFIED")
    caps = {k: v["status"] for k, v in st["capabilities"].items()}
    assert caps["CRYPTO_EXECUTION"] == caps["FX_EXECUTION"] == caps["TRADFI_EXECUTION"] == "ACTIVE"
    assert caps["UNIFIED_COLLATERAL"] == "AVAILABLE"  # the account mode READ from the broker says UTA
    unread = account_market_status(broker="bybit", environment="DEMO", permissions=PERMS, instruments=ins,
                                   transfers_in_flight=0)
    assert unread["capabilities"]["UNIFIED_COLLATERAL"]["reason"] == "ACCOUNT_TOPOLOGY_UNKNOWN"  # never guessed
    assert caps["POSITION_MODE"] == "UNSUPPORTED"
    m = st["markets"]
    assert (m["FX"]["markets_available"], m["FX"]["markets_api_tradable"], m["FX"]["markets_cati_eligible"]) == (2, 2, 0)
    assert m["FX"]["cati"]["status"] == "CERTIFICATION_NOT_READY"  # CATI eligibility needs certification
    live = account_market_status(broker="bybit", environment="LIVE", permissions=PERMS, instruments=ins,
                                 transfers_in_flight=0)
    assert live["capabilities"]["FX_EXECUTION"]["reason"] == "BROKER_EXECUTION_UNVALIDATED_FOR_LIVE"


def test_account_status_reasons_are_structured():
    from app.activation.market import account_market_status

    none = account_market_status(broker="bybit", environment="DEMO", permissions={**PERMS, "TRADE": False},
                                 instruments=[], transfers_in_flight=1, health={"quarantined": True})
    caps = {k: (v["status"], v["reason"]) for k, v in none["capabilities"].items()}
    assert caps["CRYPTO_MARKET_DATA"] == ("DATA_NOT_READY", "NO_DISCOVERED_INSTRUMENTS")
    assert caps["INTERNAL_TRANSFER"][0] == "TRANSFER_PENDING"
    quarantined = account_market_status(broker="bybit", environment="DEMO", permissions=PERMS,
                                        instruments=[parse_bybit_instrument(BYBIT_BTC)], transfers_in_flight=0,
                                        health={"quarantined": True})
    assert quarantined["capabilities"]["CRYPTO_EXECUTION"]["status"] == "RISK_BLOCKED"
    unknown = account_market_status(broker="kraken", environment="DEMO", permissions=PERMS, instruments=[])
    assert unknown["capabilities"]["CRYPTO_EXECUTION"]["status"] == "UNSUPPORTED"


def test_market_status_api_is_tenant_isolated():
    from app.activation.account_status import account_status
    from app.transfers.service import TransferAccessError

    class _Svc:
        def _auth(self, user_id, account_id):
            if (user_id, account_id) != ("alice", "acc_a"):
                raise TransferAccessError("broker account not found")
            return SimpleNamespace(broker_type="bybit", environment=SimpleNamespace(value="DEMO"),
                                   credential_version=1)

        def _evidence(self, *a):
            return None

        store = SimpleNamespace(in_flight=lambda account_id: [])

    db = SimpleNamespace(connect=lambda: (_ for _ in ()).throw(RuntimeError("no db")))
    with pytest.raises(TransferAccessError):
        account_status(db, user_id="bob", account_id="acc_a", service=_Svc())
    out = account_status(db, user_id="alice", account_id="acc_a", service=_Svc())
    assert out["broker"] == "bybit" and out["discovery"]["status"] == "DATA_NOT_READY"

    def keys(o):
        if isinstance(o, dict):
            for k, v in o.items():
                yield str(k).lower()
                yield from keys(v)
        elif isinstance(o, list):
            for v in o:
                yield from keys(v)

    assert not {k for k in keys(out) if any(t in k for t in ("api_key", "secret", "passphrase", "token"))}


# ── 28-29 auto-activation ─────────────────────────────────────────────────

def test_activation_is_derived_and_a_flag_can_only_switch_off():
    from app.activation.model import ActivationState, OperatorOverride, Prerequisite, decide, operator_override

    assert operator_override("X", {}) == OperatorOverride.AUTO
    assert operator_override("X", {"X": "1"}) == OperatorOverride.AUTO     # ON never bypasses prerequisites
    assert operator_override("X", {"X": "off"}) == OperatorOverride.FORCE_OFF
    ok = decide("C", [Prerequisite("a", True, "A_MISSING")])
    assert ok.state == ActivationState.ACTIVE
    gone = decide("C", [Prerequisite("a", False, "A_MISSING")])      # prerequisite disappears -> BLOCKED
    assert gone.state == ActivationState.BLOCKED and gone.reason == "A_MISSING"
    unknown = decide("C", [Prerequisite("a", None, "A_MISSING")])
    assert unknown.reason == "A_UNKNOWN"                              # unknown is never "satisfied"
    off = decide("C", [Prerequisite("a", True, "")], override=OperatorOverride.FORCE_OFF)
    assert off.state == ActivationState.BLOCKED and off.reason == "OPERATOR_DISABLED"
    structural = decide("C", [Prerequisite("a", False, "VENUE_API_NOT_SUPPORTED", structural=True)])
    assert structural.state == ActivationState.UNSUPPORTED


def test_cati_capabilities_auto_activate_evidence_and_block_authority(tmp_path, monkeypatch):
    from shared_lib.persistence.cati_schema import ensure_cati_schema
    from shared_lib.persistence.db import DB

    from app.activation import cati as act
    from app.trading_intelligence.governance.promotion import PromotionGovernance

    for f in act.OVERRIDE_FLAGS.values():
        monkeypatch.delenv(f, raising=False)
    assert act.cycle_shadow().active and act.capital_routing_shadow().active and act.global_market_state().active
    monkeypatch.setenv("CATI_CYCLE_SHADOW_ENABLED", "false")
    assert not act.capital_routing_shadow().active  # child follows the parent automatically
    monkeypatch.delenv("CATI_CYCLE_SHADOW_ENABLED")

    db = DB(path=str(tmp_path / "gov.db"))
    ensure_cati_schema(db)
    d = act.active_execution(db, environment="DEMO", venue="binance_usdm")
    assert not d.active and d.reasons[0] == "GOVERNANCE_PHASE_M0_NO_CATI_AUTHORITY"
    assert "RUNTIME_AUTHORITY_SWITCH_NOT_IMPLEMENTED" in d.reasons
    by = act.active_execution(db, environment="DEMO", venue="bybit_linear")
    assert "CATI_ECONOMIC_ADAPTER_NOT_VALIDATED_FOR_VENUE" in by.reasons
    gov = PromotionGovernance(db)
    gov.set_kill_switch(True, reason="t", actor_ref="op")
    assert "CATI_NEW_ENTRY_KILL_SWITCH" in act.active_execution(db, environment="DEMO").reasons
    ml = act.ml_authority("OUTCOME", registry=SimpleNamespace(promoted=lambda role: []), db=db)
    assert not ml.active and ml.reasons[0] == "NO_PROMOTED_MODEL"
    assert ml.detail["fallback"] == "DETERMINISTIC_CATI_ESTIMATOR_NEVER_V2"
    status = act.cati_status(db, registry=SimpleNamespace(promoted=lambda role: []))
    assert status["CATI_CYCLE_SHADOW"]["state"] == "ACTIVE"
    assert all(not v["state"] == "ACTIVE" for k, v in status.items() if k.startswith("CATI_ACTIVE_EXECUTION"))


def test_activation_transitions_are_observed_once_per_change(caplog):
    from app.activation import transitions
    from app.activation.model import Prerequisite, decide

    transitions.reset_for_tests()
    caplog.set_level("INFO")
    transitions.observe(decide("CAP_X", [Prerequisite("a", True, "")]))
    transitions.observe(decide("CAP_X", [Prerequisite("a", True, "")]))
    transitions.observe(decide("CAP_X", [Prerequisite("a", False, "GONE")]))
    lines = [r.message for r in caplog.records if "[ACTIVATION] capability=CAP_X" in r.message]
    assert len(lines) == 2 and "ACTIVE -> BLOCKED reason=GONE" in lines[1]


# ── 22 dynamic, frozen universes ──────────────────────────────────────────

def _ins(sym, listed, *, ac="CRYPTO", base=None):
    return parse_binance_symbol({"symbol": sym, "baseAsset": base or sym[:-4], "quoteAsset": "USDT",
                                 "marginAsset": "USDT", "status": "TRADING", "contractType": "PERPETUAL",
                                 "underlyingType": "COIN" if ac == "CRYPTO" else "EQUITY",
                                 "onboardDate": listed, "filters": []})


def test_historical_liquidity_universe_is_deterministic_and_frozen():
    from app.market_data.universe import (CERTIFICATION, FrozenUniverseError, SelectionCriteria,
                                          build_frozen_universe_manifest, exclude_stable_bases, historical_liquidity,
                                          select_universe, verify_frozen_universe)

    end = 1_790_208_000_000
    old = end - 800 * 86_400_000
    ins = [_ins("BTCUSDT", old), _ins("ETHUSDT", old), _ins("NEWUSDT", end - 10 * 86_400_000),
           _ins("USDCUSDT", old, base="USDC"), _ins("AAPLUSDT", old, ac="STOCK")]

    def kl(vol):
        return [[end - (i + 1) * 86_400_000, 1, 1, 1, 1, 1, end - i * 86_400_000 - 1, vol] for i in range(30)][::-1]

    liq = {"BTCUSDT": historical_liquidity(kl(9e9)), "ETHUSDT": historical_liquidity(kl(5e9)),
           "NEWUSDT": historical_liquidity(kl(1e9))}
    assert historical_liquidity(kl(1)[:5]).quote_volume_24h is None  # too few days -> UNKNOWN, never small
    cand, dropped = exclude_stable_bases(ins)
    assert dropped == {"USDCUSDT": "STABLECOIN_BASE"}
    crit = SelectionCriteria(min_listing_age_days=731, min_quote_volume_24h=2e6, max_spread_bps=None, target_size=10,
                             min_size=2)
    sel = select_universe(cand, liq, venue="binance_usdm", as_of_ms=end, criteria=crit, role=CERTIFICATION)
    assert sel.selected == ("BTCUSDT", "ETHUSDT")
    assert sel.excluded["NEWUSDT"] == "LISTING_TOO_RECENT" and sel.excluded["AAPLUSDT"] == "ASSET_CLASS"
    m1 = build_frozen_universe_manifest(sel, ins, window_start_ms=end - 731 * 86_400_000, window_end_ms=end,
                                        timeframes=["15m"], source_provider="binance", generated_at="t1",
                                        extra_excluded=dropped, liquidity=liq)
    m2 = build_frozen_universe_manifest(sel, ins, window_start_ms=end - 731 * 86_400_000, window_end_ms=end,
                                        timeframes=["15m"], source_provider="binance", generated_at="t2",
                                        extra_excluded=dropped, liquidity=liq)
    assert m1["universe_hash"] == m2["universe_hash"]  # generation time is not identity
    assert m1["execution_authorized"] is False and m1["rejected"]["USDCUSDT"] == "STABLECOIN_BASE"
    assert verify_frozen_universe(m1) == m1["universe_hash"]
    tampered = {**m1, "selected_symbols": ["BTCUSDT", "ETHUSDT", "DOGEUSDT"]}
    with pytest.raises(FrozenUniverseError):
        verify_frozen_universe(tampered)


def test_committed_certification_universe_manifest_verifies():
    from pathlib import Path

    from app.market_data.universe import load_frozen_universe

    path = Path(__file__).resolve().parents[3] / "docs" / "research" / "cati_crypto_universe_binance_v1.json"
    m = load_frozen_universe(str(path))
    assert m["role"] == "CERTIFICATION_UNIVERSE" and len(m["selected_symbols"]) >= 100
    assert m["window_end_ms"] - m["window_start_ms"] == 731 * 86_400_000
    assert {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"} <= set(m["selected_symbols"])
    assert all(x["asset_class"] == "CRYPTO" and x["settlement_asset"] == "USDT" for x in m["members"])
    assert m["execution_authorized"] is False and "survivorship_bias" in m


# ── 23-27 FX reference ingestion, provenance, resampling, reference vs execution ─

def _bi5(records):
    raw = b"".join(struct.pack(">IIIIIf", *r) for r in records)
    return lzma.compress(raw, format=lzma.FORMAT_ALONE)


def test_dukascopy_hour_file_decode_refuses_out_of_month_records_and_drops_closed_padding():
    from app.market_data import fx_reference as fx

    body = _bi5([(0, 117000, 117100, 116900, 117200, 10.0), (3600, 117100, 117100, 117100, 117100, 0.0)])
    rows = fx.decode_hour_file(body, year=2026, month=8, point=1e5)
    assert rows[0]["open_time"] == fx.month_start_ms(2026, 8) and rows[0]["close"] == 1.171
    assert len(fx.drop_flat_closed_bars(rows)) == 1  # flat zero-volume padding is not a quote
    bad = _bi5([(40 * 86_400, 1, 1, 1, 1, 1.0)])
    with pytest.raises(ValueError):
        fx.decode_hour_file(bad, year=2026, month=8, point=1e5)


def test_fx_quotes_keep_missing_sides_unavailable_and_resample_deterministically(tmp_path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.market_data_schema import ensure_market_data_schema

    from app.market_data import fx_reference as fx
    from app.market_data.store import MarketDataStore

    t0 = int(datetime(2026, 9, 22, 10, tzinfo=timezone.utc).timestamp() * 1000)
    bid = [{"open_time": t0 + i * 60_000, "open": 1.1, "high": 1.2, "low": 1.0, "close": 1.1 + i * 1e-5, "volume": 1.0}
           for i in range(30)]
    ask = [dict(r, close=r["close"] + 2e-5) for r in bid[:29]]  # the last minute has no ASK
    rows = fx.merge_sides(bid, ask, pair="EURUSD")
    assert rows[-1]["ask_close"] is None and "mid_close" not in rows[-1]
    db = DB(path=str(tmp_path / "fx.db"))
    ensure_market_data_schema(db)
    MarketDataStore(db).write_fx_quotes("dukascopy", "EURUSD", "EUR", "USD", "1m", rows, source_version="v")
    with db.connect() as c:
        last = c.execute("SELECT spread_close, mid_close, price_kind, source_version FROM fx_reference_quotes "
                         "ORDER BY open_time DESC LIMIT 1").fetchone()
        c.execute("INSERT INTO fx_reference_ingest_log VALUES ('dukascopy','EURUSD','1m','2026-09-26','BID',"
                  "'EMPTY',0,'MARKET_CLOSED_SATURDAY',1)")
        with pytest.raises(Exception):  # the status vocabulary is enforced
            c.execute("INSERT INTO fx_reference_ingest_log VALUES ('d','P','1m','x','BID','ZERO',0,NULL,1)")
    assert last[0] is None and last[1] is None  # never a zero spread / fabricated mid
    assert tuple(last[2:]) == ("REFERENCE_MARKET_PRICE", "v")
    fifteen = fx.resample_quotes(rows, 15)
    assert len(fifteen) == 2 and fifteen[1]["ask_close"] is None  # a window missing a side keeps it None
    assert fx.resample_quotes(rows, 15) == fifteen


def test_reference_market_is_linked_to_but_never_equal_to_the_execution_instrument():
    from app.market_data.reference_mapping import link_for, reference_market_for, tracking_statistics

    eur = parse_bybit_instrument(BYBIT_EURUSD)
    ref = reference_market_for(eur)
    assert (ref.reference_market_id, ref.provider_pair) == ("FX:EUR/USD", "EURUSD")
    link = link_for(eur)
    assert link.execution_instrument_id == "bybit_linear:EURUSDUSDT" and link.stablecoin_settlement_proxy
    assert link.tracking_basis == "PERPETUAL_WITH_FUNDING_VS_OTC_SPOT"
    assert reference_market_for(parse_bybit_instrument(BYBIT_BTC)) is None
    venue = [{"open_time": i, "close": 1.1001} for i in range(40)]
    refb = [{"open_time": i, "mid_close": 1.1} for i in range(40) if i % 7]  # closed-market gaps stay gaps
    st = tracking_statistics(venue, refb, timeframe="15m")
    assert st.status == "AVAILABLE" and st.aligned_bars == len(refb) and round(st.median_abs_bps, 3) == 0.909
    few = tracking_statistics(venue[:5], refb, timeframe="15m")
    assert few.status == "UNAVAILABLE" and few.median_abs_bps is None and few.reason.startswith("INSUFFICIENT")


def test_fx_script_candidate_pairs_are_market_convention_and_unvalidated_scales_excluded():
    import importlib.util
    from pathlib import Path

    p = Path(__file__).resolve().parents[3] / "scripts" / "acquire_fx_reference_dataset.py"
    spec = importlib.util.spec_from_file_location("fxacq", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    pairs = mod.candidate_pairs()
    assert "EURUSD" in pairs and "USDJPY" in pairs and "GBPAUD" in pairs and "USDEUR" not in pairs
    assert len(pairs) > 28  # not artificially limited to 28 majors/crosses
    assert not any(c in p for p in pairs for c in mod.UNVALIDATED_SCALE)


# ── 35 holdout / certification wiring ─────────────────────────────────────

def test_new_modules_never_open_a_holdout_or_set_execution():
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    files = list((root / "app" / "activation").glob("*.py")) + [
        root / "app/market_data/reference_mapping.py", root / "app/exchange/canonical_registry.py",
        root / "app/trading_intelligence/market_state/global_state.py",
        root / "app/trading_intelligence/market_state/global_state_store.py",
        root / "app/api/multi_asset_status.py"]
    text = "\n".join(f.read_text(encoding="utf-8") for f in files)
    assert not re.search(r"HoldoutRegistry\(|\.open\(\s*holdout|holdouts\.open|open_holdout\s*=\s*True", text)
    assert not re.search(r"setenv\(|os\.environ\[[^\]]+\]\s*=", text)
    assert "withdraw(" not in text.lower()


def test_certification_cli_pins_a_frozen_universe(tmp_path):
    from app.trading_intelligence.research.certification.cli import _universe, build_parser

    from app.market_data.universe import CERTIFICATION, SelectionCriteria, build_frozen_universe_manifest, \
        select_universe

    end = 1_790_208_000_000
    ins = [_ins("BTCUSDT", end - 900 * 86_400_000)]
    liq = {"BTCUSDT": SimpleNamespace(quote_volume_24h=1e9, spread_bps=None)}
    sel = select_universe(ins, liq, venue="binance_usdm", as_of_ms=end, role=CERTIFICATION,
                          criteria=SelectionCriteria(min_size=1, max_spread_bps=None))
    m = build_frozen_universe_manifest(sel, ins, window_start_ms=end - 731 * 86_400_000, window_end_ms=end,
                                       timeframes=["15m"], source_provider="binance", generated_at="t")
    path = tmp_path / "u.json"
    path.write_text(json.dumps(m))
    args = build_parser().parse_args(["plan", "--db", "x.db", "--universe-manifest", str(path)])
    got = _universe(args)
    assert args.symbols == "BTCUSDT" and got["universe_hash"] == m["universe_hash"]
    assert args.start == "2024-09-23T00:00:00" and args.end.startswith("2026-09-23T23:59:59")
    path.write_text(json.dumps({**m, "selected_symbols": ["ETHUSDT"]}))
    with pytest.raises(Exception):
        _universe(build_parser().parse_args(["plan", "--universe-manifest", str(path)]))
