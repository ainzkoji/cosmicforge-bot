"""Phase 4: venue-aware market data, causal resampling, quality, FX reference
ingestion, divergence, dynamic universes and immutable manifests."""
from __future__ import annotations

import lzma
import sqlite3
import struct
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from shared_lib.persistence.db import DB
from shared_lib.persistence.market_data_schema import ensure_market_data_schema

from app.market_data import fx_reference as fx
from app.market_data.divergence import compute_divergence
from app.market_data.quality import check_fx_quotes, check_series
from app.market_data.resample import derive_causal, resample_all
from app.market_data.store import MarketDataStore, MissingReasonRequired, SeriesId
from app.market_data.universe import (
    RESEARCH, SelectionCriteria, deep_subset, execution_eligibility, persist_universe_manifest, select_universe,
)
from app.market_data import venue_history as vh

M = 60_000
T0 = int(datetime(2026, 1, 5, tzinfo=timezone.utc).timestamp() * 1000)  # a Monday


def bars(n, start=T0, price=100.0):
    return [[start + i * M, f"{price + i}", f"{price + i + 1}", f"{price + i - 1}", f"{price + i + 0.5}", "10",
             start + (i + 1) * M - 1] for i in range(n)]


@pytest.fixture
def db(tmp_path):
    d = DB(path=str(tmp_path / "md.db"))
    ensure_market_data_schema(d)
    return d


# ── 4H resampling ──────────────────────────────────────────────────────────

def test_causal_resampling_never_uses_future_bars():
    rows = bars(60)
    full = derive_causal(rows, "15m")
    assert len(full) == 4
    cut = derive_causal(rows, "15m", as_of_ms=T0 + 31 * M)  # 3rd bar closes at T0+45m-1
    assert len(cut) == 2 and all(r[6] <= T0 + 31 * M for r in cut)
    gap = rows[:10] + rows[11:]  # one missing minute kills that 15m window (never approximated)
    assert len(derive_causal(gap, "15m")) == 3
    a, b = resample_all(rows, ["5m", "15m", "1h"]), resample_all(rows, ["5m", "15m", "1h"])
    assert a["15m"]["hash"] == b["15m"]["hash"] and a["1h"]["derived_from_hash"] == a["5m"]["derived_from_hash"]


# ── 4I quality ─────────────────────────────────────────────────────────────

def test_quality_extended_checks():
    rows = bars(30)
    ok = check_series(rows, symbol="BTCUSDT", timeframe="1m", sources=["binance"] * 30, listed_at_ms=T0)
    assert ok.is_usable
    bad = check_series(rows, symbol="BTCUSDT", timeframe="1m", sources=["binance"] * 15 + ["bybit"] * 15,
                       listed_at_ms=T0 + 10 * M, delisted_at_ms=T0 + 20 * M)
    assert bad.source_switches == 1 and bad.before_listing == 10 and bad.after_delisting == 10 and not bad.is_usable
    mis = [[r[0] + 1] + r[1:6] + [r[6] + 1] for r in rows]
    assert check_series(mis, symbol="X", timeframe="1m").misaligned_timestamps == 30


def test_fx_impossible_spread_detected():
    q = [{"open_time": T0 + i * M, "bid_close": 1.1, "ask_close": 1.0999 if i == 3 else 1.1001, "mid_close": 1.1,
          "bid_open": 1.1, "bid_high": 1.1, "bid_low": 1.1, "provider": "p"} for i in range(5)]
    assert check_fx_quotes(q, pair="EURUSD", timeframe="1m").impossible_spreads == 1


# ── 4A store: venue separation + UNAVAILABLE semantics ──────────────────────

def test_venues_never_merge_and_missing_is_never_zero(db):
    store = MarketDataStore(db)
    for venue in ("binance_usdm", "bybit_linear", "bingx_swap"):
        store.write_candles(SeriesId(venue, "BTCUSDT", "BTC/USDT:PERP", "CRYPTO", "PERPETUAL", "public_klines"), "1m", bars(3))
    with db.connect() as c:
        assert c.execute("SELECT COUNT(DISTINCT venue) FROM market_candles WHERE venue_symbol='BTCUSDT'").fetchone()[0] == 3
    sid = SeriesId("bingx_swap", "BTCUSDT", "BTC/USDT:PERP", "CRYPTO", "PERPETUAL", "public")
    with pytest.raises(MissingReasonRequired):
        store.record_feature(sid, "open_interest", T0)
    store.record_feature(sid, "open_interest", T0, unavailable_reason="VENUE_HAS_NO_OI_HISTORY_ENDPOINT")
    row = store.feature_at(venue="bingx_swap", venue_symbol="BTCUSDT", feature="open_interest", as_of_ms=T0 + 1)
    assert row["status"] == "UNAVAILABLE" and row["value"] is None
    with db.connect() as c, pytest.raises(sqlite3.IntegrityError):  # the schema itself refuses "unavailable = 0"
        c.execute("INSERT INTO market_feature_observations (venue, venue_symbol, canonical_symbol, asset_class, feature,"
                  " observed_at, value, status, source, ingested_at) VALUES ('v','s','c','CRYPTO','funding_rate',1,0,"
                  "'UNAVAILABLE','x',1)")
    store.record_feature(sid, "funding_rate", T0 + 5, value=0.0001)
    assert store.feature_at(venue="bingx_swap", venue_symbol="BTCUSDT", feature="funding_rate", as_of_ms=T0 + 4) is None


# ── 4E/4F FX reference ─────────────────────────────────────────────────────

def _bi5(records, point):
    raw = b"".join(struct.pack(">IIIIIf", sec, int(round(o * point)), int(round(c * point)), int(round(lo * point)),
                               int(round(hi * point)), v) for sec, o, c, lo, hi, v in records)
    return lzma.compress(raw, format=lzma.FORMAT_ALONE)


def test_dukascopy_decode_merge_and_ingest(db):
    day = date(2026, 1, 5)
    bid = _bi5([(0, 1.1000, 1.1002, 1.0999, 1.1003, 5.0), (60, 1.1002, 1.1004, 1.1001, 1.1005, 3.0)], 1e5)
    ask = _bi5([(0, 1.1001, 1.1003, 1.1000, 1.1004, 5.0)], 1e5)
    urls = []

    def fetch(url):
        urls.append(url)
        return bid if "/BID_" in url else ask

    prov = fx.DukascopyProvider(fetch=fetch)
    q = prov.minute_quotes("EURUSD", day)
    assert "/EURUSD/2026/00/05/BID_candles_min_1.bi5" in urls[0]  # zero-based month
    assert q[0]["bid_close"] == pytest.approx(1.1002) and q[0]["ask_close"] == pytest.approx(1.1003)
    assert q[1]["ask_close"] is None and "mid_close" not in q[1]  # missing side stays missing
    assert fx.point_for("USDJPY") == 1e3
    store = MarketDataStore(db)
    counts = fx.ingest(prov, store, ["EURUSD"], day, day)
    assert counts["EURUSD"] == 2
    ref = store.fx_reference_at(pair="EURUSD", timeframe="1m", as_of_ms=T0 + 2 * M)
    assert ref["price_kind"] == "REFERENCE_MARKET_PRICE" and ref["provider"] == "dukascopy"
    assert store.fx_reference_at(pair="EURUSD", timeframe="1m", as_of_ms=T0 + 30_000) is None  # bar not closed yet


def test_fx_sessions_and_quote_resampling():
    assert fx.fx_session(T0 + 13 * 3_600_000) == "LONDON_NY_OVERLAP"
    assert fx.fx_session(int(datetime(2026, 1, 10, 12, tzinfo=timezone.utc).timestamp() * 1000)) == "CLOSED"
    q = [{"open_time": T0 + i * M, "bid_open": 1, "bid_high": 1 + i, "bid_low": 1, "bid_close": 1 + i / 10,
          "ask_open": 1, "ask_high": 1.1 + i, "ask_low": 1, "ask_close": 1.1 + i / 10, "volume": 1} for i in range(5)]
    out = fx.resample_quotes(q, 5)
    assert len(out) == 1 and out[0]["bid_high"] == 5 and out[0]["mid_close"] == pytest.approx((1.4 + 1.5) / 2)
    assert fx.resample_quotes(q[:4], 5) == []


# ── 4G divergence ──────────────────────────────────────────────────────────

def test_venue_reference_divergence():
    ref = {"open_time": T0, "mid_close": 1.1000, "bid_close": 1.0999, "ask_close": 1.1001}
    d = compute_divergence(as_of_ms=T0 + M, reference=ref, venue_bid=1.1010, venue_ask=1.1012, venue_mark=1.1011)
    assert d.status == "AVAILABLE" and d.mid_vs_reference_bps == pytest.approx(10.0, abs=0.01) and d.stablecoin_proxy
    assert compute_divergence(as_of_ms=T0, reference=None).reason == "REFERENCE_UNAVAILABLE"
    assert compute_divergence(as_of_ms=T0 + 60 * M, reference=ref, venue_mark=1.1).reason == "REFERENCE_STALE"
    assert compute_divergence(as_of_ms=T0 + M, reference=ref).reason == "VENUE_PRICE_UNAVAILABLE"


# ── 4B/4C/4J universes ─────────────────────────────────────────────────────

def _ins(sym, qv, listed_days_ago=900, ac="CRYPTO", product="PERPETUAL", settle="USDT", tradable=True):
    now = T0
    return SimpleNamespace(venue_symbol=sym, asset_class=ac, product_type=product, settlement_asset=settle,
                           api_tradable=tradable, listed_at_ms=now - listed_days_ago * 86_400_000), SimpleNamespace(
        quote_volume_24h=qv, spread_bps=2.0)


def test_dynamic_universe_selection_is_deterministic_and_reasoned(db):
    pairs = [_ins(f"C{i:03d}USDT", 10_000_000 + i) for i in range(160)]
    pairs += [_ins("NEWUSDT", 9e9, listed_days_ago=10), _ins("THINUSDT", 1_000), _ins("UNKUSDT", None),
              _ins("EURUSDT", 9e9, ac="FX"), _ins("DEADUSDT", 9e9, tradable=False)]
    instruments = [p[0] for p in pairs]
    stats = {p[0].venue_symbol: p[1] for p in pairs}
    sel = select_universe(instruments, stats, venue="binance_usdm", as_of_ms=T0)
    assert len(sel.selected) == 150 and sel.shortfall is None and sel.selected[0] == "C159USDT"
    assert sel.excluded["NEWUSDT"] == "LISTING_TOO_RECENT" and sel.excluded["THINUSDT"] == "LOW_LIQUIDITY"
    assert sel.excluded["UNKUSDT"] == "LIQUIDITY_UNKNOWN" and sel.excluded["EURUSDT"] == "ASSET_CLASS"
    assert sel.excluded["DEADUSDT"] == "NOT_TRADING" and sel.excluded["C000USDT"] == "RANK_BELOW_TARGET"
    assert select_universe(instruments, stats, venue="binance_usdm", as_of_ms=T0).manifest_hash == sel.manifest_hash
    deep = deep_subset(sel, 35)
    assert len(deep.selected) == 35 and deep.criteria["parent_manifest"] == sel.manifest_hash
    persist_universe_manifest(db, sel)
    persist_universe_manifest(db, sel)  # idempotent
    with db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM universe_manifests").fetchone()[0] == 1
    with db.connect() as c, pytest.raises(sqlite3.IntegrityError):
        c.execute("UPDATE universe_manifests SET role='EXECUTION_UNIVERSE'")
    small = select_universe(instruments[:20], stats, venue="v", as_of_ms=T0, criteria=SelectionCriteria())
    assert small.shortfall == "ONLY_20_OF_MIN_100"


def test_research_universe_is_not_execution_eligible():
    ok, missing = execution_eligibility({"data_sufficient": True, "liquid": True})
    assert not ok and "NOT_CERTIFIED_SCOPE" in missing and "NOT_ACCOUNT_CAPABLE" in missing
    assert execution_eligibility({k: True for k in ("data_sufficient", "economics_available", "calibrated", "liquid",
                                                    "venue_capable", "account_capable", "certified_scope")})[0]


# ── venue history fetchers (recorded shapes) ───────────────────────────────

def test_venue_history_fetchers_paginate_and_report_unavailable():
    calls = []

    def get(url, params):
        calls.append((url, dict(params)))
        if url.endswith("/fapi/v1/klines"):
            start = params["startTime"]
            if start > T0 + 2 * M:
                return []
            return [[start + i * M, "1", "2", "0.5", "1.5", "10", start + (i + 1) * M - 1, "15", 3] for i in range(2)]
        if url.endswith("/v5/market/kline"):
            return {"result": {"list": [[str(T0 + M), "1", "2", "0.5", "1.5", "10", "15"],
                                        [str(T0), "1", "2", "0.5", "1.5", "10", "15"]]}}
        raise AssertionError(url)

    k = vh.binance_klines(get, "BTCUSDT", "1m", T0, T0 + 5 * M)
    assert [r[0] for r in k] == [T0, T0 + M, T0 + 2 * M, T0 + 3 * M] and len(calls) == 3
    y = vh.bybit_klines(get, "BTCUSDT", "1m", T0, T0 + M)
    assert [r[0] for r in y] == [T0, T0 + M]  # ascending, de-duplicated
    oi = vh.binance_open_interest(get, "BTCUSDT", T0, T0 + M, now_ms=T0 + 90 * 86_400_000)
    assert isinstance(oi, vh.FeatureUnavailable) and oi.reason == "VENUE_HISTORY_LIMIT_30D"
    assert vh.bingx_open_interest().reason == "VENUE_HAS_NO_OI_HISTORY_ENDPOINT"
    assert "NO_COMPLETE_HISTORICAL_LIQUIDATION_FEED" in vh.liquidations("bybit").reason
