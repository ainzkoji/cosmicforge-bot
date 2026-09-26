import copy
import pytest
from app.market_data.quality import audit_partition, check_fx_quotes
from app.market_data.fx_universe import build_fx_universe_manifest
from app.market_data.universe import freeze_dataset_payload, verify_dataset_payload
from app.market_data.gaps import classify_fx_gap, INGEST_FAILURE, UNKNOWN_GAP, PROVIDER_OUTAGE


def rows():
    return [[t, 10., 12., 9., 11., 1., t+59999] for t in (0,60000,120000)]


def audit(data=None, **kwargs):
    return audit_partition(iter(rows() if data is None else data), symbol="EURUSD", timeframe="1m",
        start_ms=0,end_ms=180000,source="dukascopy",source_version="fixture-v1", **kwargs)


@pytest.mark.parametrize("index,value",[(2,8),(3,12),(5,-1),(1,float("nan")),(4,float("inf"))])
def test_invalid_prices_and_volume(index,value):
    data=rows();data[0][index]=value
    assert audit(data)["invalid_rows"] == 1
    assert audit(data)["status"] == "PARTIAL"


def test_streaming_chronology_duplicates_coverage_and_identity():
    good=audit()
    assert good["status"] == "COMPLETE" and good["coverage_pct"] == 100
    assert audit(acquiring=True)["status"] == "ACQUIRING"
    assert audit(failed=True)["status"] == "FAILED"
    assert audit(rows()[::-1])["out_of_order"] > 0
    assert audit([rows()[0],*rows()])["duplicate_rows"] == 1
    assert audit(rows()[:2])["missing_ranges"] == [{"start_ms":120000,"end_ms":180000,"missing_bars":1,"reason":"UNKNOWN_GAP"}]
    assert good["partition_hash"] == audit()["partition_hash"]
    changed=rows();changed[1][4]=10.5
    assert good["partition_hash"] != audit(changed)["partition_hash"]
    assert good["partition_hash"] != audit(venue="other")["partition_hash"]


def frozen(part=None, created_at="today"):
    universe=build_fx_universe_manifest(provider="dukascopy",members=[{"pair":"EURUSD","base":"EUR","quote":"USD",
        "scale_status":"PASS","cross_rate_status":"PASS"}],excluded={},window_start_ms=0,window_end_ms=180000,
        scale_qa_version="1",gap_policy_version="1",source_versions=["fixture-v1"],generated_at="x",code_commit="abc")
    return freeze_dataset_payload(universe=universe,partitions=[part or audit()],metadata_hash="meta",code_commit="abc",
        product_type="REFERENCE",base_interval="1m",resampling_policy_version="1",gap_policy_version="2",created_at=created_at)


def test_freeze_exact_membership_tampering_and_clock_independence():
    a=frozen();b=frozen(created_at="tomorrow")
    assert a["manifest_hash"] == b["manifest_hash"] == verify_dataset_payload(a)
    a["symbols"].append("GBPUSD")
    with pytest.raises(ValueError):verify_dataset_payload(a)
    with pytest.raises(ValueError,match="MEMBERSHIP"):frozen({**audit(),"symbol":"GBPUSD"})
    with pytest.raises(ValueError,match="SUBSTITUTION"):frozen({**audit(),"source":"other"})


@pytest.mark.parametrize("status",["ACQUIRING","PARTIAL","FAILED"])
def test_incomplete_data_never_frozen(status):
    with pytest.raises(ValueError,match="CANNOT_FREEZE"):frozen({**audit(),"status":status})


@pytest.mark.parametrize("field,value",[("bid_high",.9),("ask_low",1.5),("ask_close",1.0)])
def test_fx_independent_side_ohlc_and_spread(field,value):
    q={"open_time":0,**{f"bid_{k}":1.1 for k in ("open","high","low","close")},
                        **{f"ask_{k}":1.1001 for k in ("open","high","low","close")}}
    q[field]=value
    assert not check_fx_quotes([q],pair="EURUSD",timeframe="1m").is_usable


def test_outage_needs_specific_evidence():
    from datetime import datetime,timezone
    t=int(datetime(2025,7,15,12,tzinfo=timezone.utc).timestamp()*1000)
    for status,expected in (("FAILED",INGEST_FAILURE),("EMPTY",UNKNOWN_GAP),("PROVIDER_OUTAGE",PROVIDER_OUTAGE)):
        assert classify_fx_gap(t,t,60000,ingest_status={"2025-07-15":status}) == expected


# -- store identity, provenance and membership (13.3, 13.8-13.10) ----------------------------------------------

@pytest.fixture
def store(tmp_path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.market_data_schema import ensure_market_data_schema
    from app.market_data.store import MarketDataStore
    db = DB(path=str(tmp_path / "s13.db"))
    ensure_market_data_schema(db)
    return MarketDataStore(db)


def sid(venue="binance_usdm", source="public_klines"):
    from app.market_data.store import SeriesId
    return SeriesId(venue, "BTCUSDT", "BTC/USDT:PERP", "CRYPTO", "PERPETUAL", source)


def test_repeated_observation_counted_once_and_venues_never_collapse(store):
    assert store.write_candles(sid(), "1m", rows()) == 3
    assert store.write_candles(sid(), "1m", rows()) == 0  # same identity again: nothing new
    assert store.write_candles(sid("bybit_linear"), "1m", rows()) == 3  # same symbol/time, other venue: kept
    assert len(store.read_candles(venue="binance_usdm", venue_symbol="BTCUSDT", timeframe="1m")) == 3
    assert len(store.read_candles(venue="bybit_linear", venue_symbol="BTCUSDT", timeframe="1m")) == 3


def test_invalid_candles_rejected_never_clamped(store):
    bad = rows(); bad[1][2] = 8.0  # high below open/close
    with pytest.raises(ValueError, match="CANDLE_QUALITY_REJECTED"):
        store.write_candles(sid(), "1m", bad)
    assert store.read_candles(venue="binance_usdm", venue_symbol="BTCUSDT", timeframe="1m") == []


def test_no_silent_source_substitution_on_read(store):
    store.write_candles(sid(source="archive"), "1m", rows())
    store.write_candles(sid(source="public_klines"), "1m", rows())
    with pytest.raises(ValueError, match="AMBIGUOUS"):
        store.read_candles(venue="binance_usdm", venue_symbol="BTCUSDT", timeframe="1m")
    assert len(store.read_candles(venue="binance_usdm", venue_symbol="BTCUSDT", timeframe="1m", source="archive")) == 3
    q = {"open_time": 0, **{f"bid_{k}": 1.1 for k in ("open", "high", "low", "close")},
         **{f"ask_{k}": 1.1001 for k in ("open", "high", "low", "close")}}
    for provider in ("dukascopy", "secondary"):
        store.write_fx_quotes(provider, "EURUSD", "EUR", "USD", "1m", [q], source_version="t")
    with pytest.raises(ValueError, match="PROVIDER_REQUIRED"):
        store.fx_reference_at(pair="EURUSD", timeframe="1m", as_of_ms=120_000)
    assert store.fx_reference_at(pair="EURUSD", timeframe="1m", as_of_ms=120_000, provider="dukascopy")["provider"] == "dukascopy"


def test_fx_out_of_order_rejected_not_reordered(store):
    q = lambda t: {"open_time": t, **{f"bid_{k}": 1.1 for k in ("open", "high", "low", "close")},  # noqa: E731
                   **{f"ask_{k}": 1.1001 for k in ("open", "high", "low", "close")}}
    with pytest.raises(ValueError, match="OUT_OF_ORDER"):
        store.write_fx_quotes("dukascopy", "EURUSD", "EUR", "USD", "1m", [q(60_000), q(0)], source_version="t")


def test_membership_source_and_policy_change_identity():
    base = dict(provider="dukascopy", excluded={}, window_start_ms=0, window_end_ms=180000, scale_qa_version="1",
                gap_policy_version="1", source_versions=["fixture-v1"], generated_at="x", code_commit="abc")
    m = lambda p: {"pair": p, "base": p[:3], "quote": p[3:], "scale_status": "PASS", "cross_rate_status": "PASS"}  # noqa: E731
    one = build_fx_universe_manifest(members=[m("EURUSD")], **base)["universe_hash"]
    assert one == build_fx_universe_manifest(members=[m("EURUSD")], **{**base, "generated_at": "later"})["universe_hash"]
    assert one != build_fx_universe_manifest(members=[m("EURUSD"), m("GBPUSD")], **base)["universe_hash"]
    assert one != build_fx_universe_manifest(members=[m("EURUSD")], **{**base, "provider": "other"})["universe_hash"]
    assert one != build_fx_universe_manifest(members=[m("EURUSD")], **{**base, "gap_policy_version": "2"})["universe_hash"]
    a = frozen()
    assert a["manifest_hash"] != freeze_dataset_payload(**{**_freeze_args(), "metadata_hash": "meta2"})["manifest_hash"]
    assert audit()["partition_hash"] != audit_partition(iter(rows()), symbol="EURUSD", timeframe="1m", start_ms=0,
        end_ms=180000, source="other", source_version="fixture-v1")["partition_hash"]


def _freeze_args():
    universe = build_fx_universe_manifest(provider="dukascopy", members=[{"pair": "EURUSD", "base": "EUR", "quote": "USD",
        "scale_status": "PASS", "cross_rate_status": "PASS"}], excluded={}, window_start_ms=0, window_end_ms=180000,
        scale_qa_version="1", gap_policy_version="1", source_versions=["fixture-v1"], generated_at="x", code_commit="abc")
    return dict(universe=universe, partitions=[audit()], metadata_hash="meta", code_commit="abc", product_type="REFERENCE",
                base_interval="1m", resampling_policy_version="1", gap_policy_version="2", created_at="t")


def test_newly_discovered_symbol_stays_outside_the_frozen_run():
    args = _freeze_args()
    extra = {**audit(), "symbol": "NEWUSD"}
    with pytest.raises(ValueError, match="MEMBERSHIP"):
        freeze_dataset_payload(**{**args, "partitions": [audit(), extra]})


def test_crypto_gap_taxonomy_needs_evidence():
    from app.market_data.gaps import (LISTING_AGE, MARKET_HALT, PROVIDER_FAILURE, VENUE_OUTAGE,
                                      classify_crypto_gap)
    assert classify_crypto_gap(0, 10, listed_at_ms=100) == LISTING_AGE
    assert classify_crypto_gap(20, 30, failed_periods=[(0, 50)]) == PROVIDER_FAILURE
    assert classify_crypto_gap(20, 30, evidence={"MARKET_HALT": [(0, 50)]}) == MARKET_HALT
    assert classify_crypto_gap(20, 30, evidence={"VENUE_OUTAGE": [(0, 50)]}) == VENUE_OUTAGE
    assert classify_crypto_gap(20, 30) == UNKNOWN_GAP


def test_weekend_holiday_and_no_file_fx_gaps():
    from datetime import datetime, timezone
    from app.market_data.gaps import HOLIDAY_CLOSED, PROVIDER_NO_FILE, WEEKEND_CLOSED
    ms = lambda *a: int(datetime(*a, tzinfo=timezone.utc).timestamp() * 1000)  # noqa: E731
    assert classify_fx_gap(ms(2025, 7, 19, 12), ms(2025, 7, 19, 14), 3_600_000) == WEEKEND_CLOSED
    assert classify_fx_gap(ms(2025, 12, 25, 10), ms(2025, 12, 25, 12), 3_600_000) == HOLIDAY_CLOSED
    t = ms(2025, 7, 15, 12)
    assert classify_fx_gap(t, t, 60000, ingest_status={"2025-07-15": "NO_FILE"}) == PROVIDER_NO_FILE


def test_wide_spread_is_stored_and_flagged_but_crossed_quote_rejected(store):
    wide = {"open_time": 0, **{f"bid_{k}": 1.0 for k in ("open", "high", "low", "close")},
            **{f"ask_{k}": 1.05 for k in ("open", "high", "low", "close")}}  # ~490 bps: a rollover fact, not a fault
    assert store.write_fx_quotes("dukascopy", "USDTRY", "USD", "TRY", "1m", [wide], source_version="t") == 1
    assert check_fx_quotes([wide], pair="USDTRY", timeframe="1m").impossible_spreads == 1  # QA still flags it
    crossed = {**wide, "open_time": 60_000, "ask_close": 0.99}
    with pytest.raises(ValueError, match="QUALITY_REJECTED"):
        store.write_fx_quotes("dukascopy", "USDTRY", "USD", "TRY", "1m", [crossed], source_version="t")
