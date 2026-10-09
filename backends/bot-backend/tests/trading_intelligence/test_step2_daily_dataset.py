"""Section H, Step 2.3: the public-archive client and the daily research dataset, fully offline.

A small in-memory archive (bucket listing + zip files + .CHECKSUM files + exchange metadata) stands in for
data.binance.vision, so every failure mode can be produced on demand and no test touches the network.
"""
from __future__ import annotations

import hashlib
import io
import json
import urllib.parse
import zipfile

import numpy as np
import pytest

from app.market_data import binance_archive as BA, daily_dataset as DD

DAY = BA.DAY_MS
D2020 = DD.day_index("2020-01-01")


def zip_bytes(name: str, lines) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        info = zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))     # fixed: the same bytes every time
        zf.writestr(info, "\n".join(lines) + "\n")
    return buf.getvalue()


def kline_line(day: int, o=10.0, h=11.0, l=9.0, c=10.5, v=100.0, qv=1000.0, n=7, unit="ms") -> str:
    ts = day * DAY
    ts = {"ms": ts, "us": ts * 1000, "s": ts // 1000}[unit]
    return f"{ts},{o},{h},{l},{c},{v},{ts + DAY - 1},{qv},{n},1,1,0"


class FakeArchive:
    """``get(url)`` for the listing endpoint, the file host and exchangeInfo. Counts every request."""

    def __init__(self, page: int = 3):
        self.files, self.calls, self.page, self.exchange_info = {}, [], page, {"symbols": []}

    def add(self, key: str, body: bytes, *, checksum: str = "") -> None:
        self.files[key] = body
        name = key.rsplit("/", 1)[-1]
        self.files[key + ".CHECKSUM"] = f"{checksum or hashlib.sha256(body).hexdigest()}  {name}\n".encode()

    def klines(self, symbol: str, month: str, lines, header=False, **kw) -> str:
        key = f"{BA.KLINES_ROOT}{symbol}/1d/{symbol}-1d-{month}.zip"
        head = ["open_time,open,high,low,close,volume,close_time,quote_volume,count,tbv,tbqv,ignore"] if header else []
        self.add(key, zip_bytes(f"{symbol}-1d-{month}.csv", head + list(lines)), **kw)
        return key

    def funding(self, symbol: str, month: str, rows) -> str:
        key = f"{BA.FUNDING_ROOT}{symbol}/{symbol}-fundingRate-{month}.zip"
        lines = ["calc_time,funding_interval_hours,last_funding_rate"] + [f"{t},{h},{r}" for t, h, r in rows]
        self.add(key, zip_bytes(f"{symbol}-fundingRate-{month}.csv", lines))
        return key

    def get(self, url: str) -> bytes:
        self.calls.append(url)
        if url.startswith(BA.LISTING_URL):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
            prefix, marker = q["prefix"][0], q.get("marker", [""])[0]
            names = sorted({k[len(prefix):].split("/", 1)[0] + ("/" if "/" in k[len(prefix):] else "")
                            for k in self.files if k.startswith(prefix)})
            entries = [prefix + n for n in names if prefix + n > marker]
            chunk, more = entries[:self.page], len(entries) > self.page
            body = "".join(f"<CommonPrefixes><Prefix>{e}</Prefix></CommonPrefixes>" if e.endswith("/") else
                           f"<Contents><Key>{e}</Key><Size>{len(self.files[e])}</Size>"
                           f"<LastModified>2026-10-01T00:00:00.000Z</LastModified></Contents>" for e in chunk)
            tail = f"<NextMarker>{chunk[-1]}</NextMarker>" if more else ""
            return (f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>'
                    f'{"true" if more else "false"}</IsTruncated>{tail}{body}</ListBucketResult>').encode()
        if url == DD.EXCHANGE_INFO_URL:
            return json.dumps(self.exchange_info).encode()
        key = url[len(BA.ARCHIVE_URL) + 1:]
        if key not in self.files:
            raise BA.ArchiveError(f"not in the archive: {url}")
        return self.files[key]

    def data_requests(self):
        return [u for u in self.calls if u.startswith(BA.ARCHIVE_URL)]


@pytest.fixture()
def archive():
    a = FakeArchive()
    jan = [kline_line(D2020 + i, c=10.0 + i, h=11.0 + i, qv=1000.0 + i) for i in range(31)]
    feb = [kline_line(D2020 + 31 + i, c=50.0, h=51.0) for i in range(29)]
    a.klines("BTCUSDT", "2020-01", jan)                                   # old files: no header
    a.klines("BTCUSDT", "2020-02", feb, header=True)                      # newer files: a header row
    a.klines("LUNAUSDT", "2020-01", jan[:20])                             # a contract that ended on 20 January
    a.klines("AAAUSDTSETTLED", "2020-01", jan[:5])                        # settled and later replaced
    a.klines("BTCUSDT_200327", "2020-01", jan)                            # a dated delivery contract: out of scope
    a.klines("BTCUSDC", "2020-01", jan)                                   # margined in another asset: out of scope
    a.klines("BTCUSDT", "2019-12", jan)                                   # outside the dataset months
    eight = [(d * DAY + h * 3_600_000 + 4, 8, 0.0001 if h else -0.0002) for d in range(D2020, D2020 + 31) for h in (0, 8, 16)]
    a.funding("BTCUSDT", "2020-01", eight)
    a.funding("LUNAUSDT", "2020-01", [r for r in eight if r[0] // DAY < D2020 + 10])     # funding stops early
    a.exchange_info = {"symbols": [{"symbol": "BTCUSDT", "contractType": "PERPETUAL", "underlyingType": "COIN",
                                    "underlyingSubType": ["PoW"], "status": "TRADING", "baseAsset": "BTC",
                                    "quoteAsset": "USDT", "marginAsset": "USDT", "onboardDate": 1567965300000,
                                    "deliveryDate": 4133404800000,
                                    "filters": [{"filterType": "MARKET_LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                                                {"filterType": "MIN_NOTIONAL", "notional": "50"}]}]}
    return a


def build(archive, store, **kw):
    disc = DD.discover(get=archive.get, workers=2, first_month="2020-01", last_month="2020-02")
    sync = DD.mirror(store, disc, get=archive.get, workers=2)
    meta = DD.metadata_snapshot(get=archive.get, fetched_at="2026-10-09T00:00:00Z")
    norm = DD.normalize(store, disc, sync, meta)
    return disc, sync, meta, norm, DD.build_manifest(norm, disc, meta, **kw)


# ============================== DISCOVERY AND MIRROR ==============================
def test_discovery_reads_the_archive_listing_so_ended_contracts_are_found(archive):
    disc = DD.discover(get=archive.get, workers=2, first_month="2020-01", last_month="2020-02")
    assert sorted(disc["files"]) == ["AAAUSDTSETTLED", "BTCUSDT", "LUNAUSDT"]          # paging followed (3 per page)
    assert disc["not_in_scope"] == ["BTCUSDC", "BTCUSDT_200327"] and disc["archive_symbols"] == 5
    months = [f["key"][-11:-4] for f in disc["files"]["BTCUSDT"]["klines"]]
    assert months == ["2020-01", "2020-02"]                                            # 2019-12 is outside the window
    assert all(not f["key"].endswith(".CHECKSUM") for s in disc["files"].values() for k in s.values() for f in k)


def test_the_mirror_verifies_every_file_and_never_downloads_a_verified_file_again(archive, tmp_path):
    disc = DD.discover(get=archive.get, workers=2, first_month="2020-01", last_month="2020-02")
    first = DD.mirror(tmp_path, disc, get=archive.get, workers=2)
    assert {r["status"] for r in first} == {"DOWNLOADED"} and len(first) == 6
    key = first[0]["key"]
    assert BA.sha256_file(tmp_path / "raw" / key) == first[0]["sha256"] == hashlib.sha256(archive.files[key]).hexdigest()
    before = len(archive.data_requests())
    again = DD.mirror(tmp_path, disc, get=archive.get, workers=2)
    assert {r["status"] for r in again} == {"CACHED"} and len(archive.data_requests()) == before   # no request at all
    assert [r["sha256"] for r in again] == [r["sha256"] for r in first]


def test_an_interrupted_download_leaves_no_usable_file_and_resumes(archive, tmp_path):
    key = f"{BA.KLINES_ROOT}BTCUSDT/1d/BTCUSDT-1d-2020-01.zip"
    m = BA.ArchiveMirror(tmp_path, get=archive.get)
    m.local(key).parent.mkdir(parents=True)
    m.local(key).with_name(m.local(key).name + ".part").write_bytes(b"half a file")     # a crash mid-write
    assert m.verified_sha256(key) is None
    assert m.ensure(key)["status"] == "DOWNLOADED" and m.verified_sha256(key)
    m.local(key).write_bytes(m.local(key).read_bytes()[:-10])                            # a truncated file on disk
    assert m.verified_sha256(key) is None and m.ensure(key)["status"] == "DOWNLOADED"
    m.local(key + ".CHECKSUM").unlink()                                                  # checksum lost: not trusted
    assert m.verified_sha256(key) is None and m.ensure(key)["status"] == "DOWNLOADED"


def test_a_file_that_does_not_match_its_archive_checksum_is_refused_and_deleted(archive, tmp_path):
    bad = archive.klines("ETHUSDT", "2020-01", [kline_line(D2020)], checksum="0" * 64)
    m = BA.ArchiveMirror(tmp_path, get=archive.get)
    with pytest.raises(BA.ChecksumMismatch):
        m.ensure(bad)
    assert not m.local(bad).exists() and not m.local(bad + ".CHECKSUM").exists()
    out = m.sync([bad, f"{BA.KLINES_ROOT}BTCUSDT/1d/BTCUSDT-1d-2020-01.zip"], workers=2)
    assert [r["status"] for r in out] == ["FAILED", "DOWNLOADED"] and "checksum" in out[0]["error"]   # reported, not dropped
    archive.files[bad + ".CHECKSUM"] = b"not a checksum"
    with pytest.raises(BA.ArchiveError, match="malformed"):
        m.ensure(bad)
    with pytest.raises(BA.ArchiveError, match="not in the archive"):
        m.ensure("data/futures/um/monthly/klines/NOPE/1d/NOPE-1d-2020-01.zip")


# ============================== PARSING ==============================
def test_timestamps_are_normalized_to_utc_milliseconds_or_refused():
    ms = D2020 * DAY
    assert BA.normalize_timestamp_ms(ms) == ms == 1577836800000
    assert BA.normalize_timestamp_ms(ms * 1000) == ms and BA.normalize_timestamp_ms(ms // 1000) == ms
    assert BA.normalize_timestamp_ms(f" {ms} ") == ms
    for bad in (ms * 1000 + 7, 15778368, ms * 1_000_000):
        with pytest.raises(BA.ArchiveError):
            BA.normalize_timestamp_ms(bad)
    assert DD.day_text(DD.day_index("2024-02-29")) == "2024-02-29" and DD.day_index("1970-01-02") == 1


def test_kline_and_funding_files_parse_with_and_without_a_header(archive, tmp_path):
    m = BA.ArchiveMirror(tmp_path, get=archive.get)
    for month, n in (("2020-01", 31), ("2020-02", 29)):
        key = f"{BA.KLINES_ROOT}BTCUSDT/1d/BTCUSDT-1d-{month}.zip"
        m.ensure(key)
        rows = BA.read_kline_zip(m.local(key))
        assert len(rows) == n and rows[0][0] % DAY == 0 and isinstance(rows[0][7], int)
    assert rows[0][1:5] == (10.0, 51.0, 9.0, 50.0)
    us = archive.klines("USUSDT", "2020-01", [kline_line(D2020, unit="us")])
    m.ensure(us)
    assert BA.read_kline_zip(m.local(us))[0][0] == D2020 * DAY                          # microseconds -> milliseconds
    fkey = f"{BA.FUNDING_ROOT}BTCUSDT/BTCUSDT-fundingRate-2020-01.zip"
    m.ensure(fkey)
    f = BA.read_funding_zip(m.local(fkey))
    assert len(f) == 93 and f[0] == (D2020 * DAY + 4, 8, -0.0002)
    two = tmp_path / "two.zip"
    with zipfile.ZipFile(two, "w") as zf:
        zf.writestr("a.csv", "1")
        zf.writestr("b.csv", "2")
    with pytest.raises(BA.ArchiveError, match="exactly one CSV"):
        BA.read_kline_zip(two)


# ============================== CLEANING ==============================
def row(day, o=10.0, h=11.0, l=9.0, c=10.5, v=1.0, qv=10.0, n=1):
    return (day * DAY, o, h, l, c, v, qv, n)


def test_missing_candles_are_counted_and_never_filled():
    k = DD.clean_klines([row(0), row(1), row(4), row(5), row(9)])
    assert k["days"].tolist() == [0, 1, 4, 5, 9] and k["missing_ranges"] == [(2, 3), (6, 8)]
    assert len(k["values"]) == 5                                            # no invented rows


def test_duplicates_keep_the_first_occurrence_and_out_of_order_rows_are_sorted_and_counted():
    k = DD.clean_klines([row(2, c=10.2), row(0, c=10.0), row(1, c=10.1), row(1, c=10.9), row(3, c=10.3)])
    assert k["days"].tolist() == [0, 1, 2, 3] and k["duplicates"] == 1 and k["out_of_order"] == 1
    assert k["values"][:, 3].tolist() == [10.0, 10.1, 10.2, 10.3]            # the first day-1 row, not the second


def test_invalid_bars_are_quarantined_not_repaired():
    rows = [row(0), row(1, h=8.0), row(2, l=12.0), row(3, c=-1.0), row(4, c=float("nan")), row(5, qv=-1.0),
            (6 * DAY + 3_600_000, 10.0, 11.0, 9.0, 10.5, 1.0, 10.0, 1), row(7), row(8, v=0.0, qv=0.0)]
    k = DD.clean_klines(rows)
    assert k["days"].tolist() == [0, 7, 8] and len(k["invalid"]) == 6 and k["zero_volume_days"] == 1
    assert k["missing_ranges"] == [(1, 6)]                                   # a quarantined bar is a missing bar
    assert DD.clean_klines([])["days"].tolist() == []


def test_funding_is_split_by_time_of_day_and_gaps_are_measured():
    hour = 3_600_000
    f = DD.clean_funding([(0 * DAY + 3, 8, 0.0001), (0 * DAY + 8 * hour, 8, 0.0002), (0 * DAY + 16 * hour, 8, -0.0003),
                          (1 * DAY, 8, 0.0001), (1 * DAY, 8, 0.0009), (2 * DAY, 8, 9.0), (3 * DAY, 0, 0.0001)])
    assert f["duplicates"] == 1 and f["invalid"] == 2 and len(f["rate"]) == 4
    by = DD.funding_by_day(f)
    assert by[0] == (pytest.approx(0.0001), pytest.approx(0.0002), pytest.approx(-0.0003), 24.0, 3)
    assert by[1] == (pytest.approx(0.0001), 0.0, 0.0, 8.0, 1) and 2 not in by   # day 1 incomplete, day 2 absent


# ============================== NORMALIZE, MANIFEST, LOAD ==============================
def test_the_dataset_records_coverage_gaps_and_ended_contracts(archive, tmp_path):
    _disc, sync, meta, norm, manifest = build(archive, tmp_path)
    q = norm["symbols"]
    btc, luna, aaa = q["BTCUSDT"], q["LUNAUSDT"], q["AAAUSDTSETTLED"]
    assert (btc["first_available_day"], btc["last_available_day"]) == ("2020-01-01", "2020-02-29")
    assert btc["expected_candle_count"] == btc["actual_candle_count"] == 60 and btc["missing_candles"] == 0
    assert btc["listing_status_source"] == "EXCHANGE_METADATA_SNAPSHOT" and btc["funding_records"] == 93
    assert btc["bar_days_without_any_funding_record"] == 29                    # February has bars and no funding file
    assert luna["last_available_day"] == "2020-01-20" and luna["listing_status_source"] == "ARCHIVE_COVERAGE_ONLY"
    assert luna["bar_days_without_any_funding_record"] == 10 and luna["first_bar_day_without_funding"] == "2020-01-11"
    assert aaa["actual_candle_count"] == 5 and aaa["funding_records"] == 0
    ended = manifest["contracts_that_ended_before_coverage_end"]
    assert (ended["count"], ended["symbols"]) == (2, ["AAAUSDTSETTLED", "LUNAUSDT"])
    assert manifest["quality_totals"]["bar_days_without_any_funding_record"] == 29 + 10 + 5
    assert manifest["point_in_time_universe_method"]["delisted_contracts"].startswith("INCLUDED")
    assert manifest["point_in_time_universe_method"]["authoritative_listing_history"].startswith("NOT_AVAILABLE")
    assert any("snapshot" in x for x in manifest["known_limitations"]) and manifest["status"] == "FROZEN"
    assert manifest["raw_artifact_hashes"]["files"] == 6 == len(sync)
    assert meta["point_in_time"] is False and meta["symbols"]["BTCUSDT"]["min_notional"] == "50"


def test_building_twice_gives_the_same_hashes_and_an_edit_is_detected(archive, tmp_path):
    a = build(archive, tmp_path / "a", created_at="2026-10-09T00:00:00Z")
    b = build(archive, tmp_path / "b", created_at="2027-01-01T00:00:00Z", code_commit="other")
    ma, mb = a[4], b[4]
    assert ma["manifest_hash"] == mb["manifest_hash"] and ma["dataset_hash"] == mb["dataset_hash"]   # no wall clock inside
    assert DD.inventory_bytes(a[3]["inventory"]) == DD.inventory_bytes(b[3]["inventory"])
    assert ma["raw_artifact_hashes"]["inventory_sha256"] == hashlib.sha256(DD.inventory_bytes(a[3]["inventory"])).hexdigest()
    assert DD.verify_manifest(ma) == ma["manifest_hash"]
    edited = json.loads(json.dumps(ma))
    edited["quality_totals"]["missing_candles"] = 0
    edited["known_limitations"] = []
    with pytest.raises(ValueError, match="edited"):
        DD.verify_manifest(edited)
    # different data -> different dataset
    archive.klines("BTCUSDT", "2020-02", [kline_line(D2020 + 31 + i, c=50.0, h=51.0) for i in range(28)], header=True)
    c = build(archive, tmp_path / "c")
    assert c[4]["dataset_hash"] != ma["dataset_hash"] and c[3]["symbols"]["BTCUSDT"]["actual_candle_count"] == 59


def test_a_normalized_table_that_differs_from_the_manifest_is_refused(archive, tmp_path):
    import pandas as pd

    _d, _s, _m, norm, manifest = build(archive, tmp_path)
    coverage = {"symbols": norm["symbols"]}
    klines, funding = DD.load_tables(tmp_path, manifest, coverage)
    assert len(klines) == 60 + 20 + 5 and len(funding) == 93 + 30
    path = tmp_path / "normalized" / "klines_1d.parquet"
    tampered = pd.read_parquet(path)
    tampered.loc[tampered.index[3], "close"] *= 1.01                         # one price nudged
    tampered.to_parquet(path, index=False)
    with pytest.raises(ValueError, match="does not match the frozen manifest"):
        DD.load_tables(tmp_path, manifest, coverage)


def test_the_panel_has_nan_for_missing_days_and_funding_by_time_of_day(archive, tmp_path):
    _d, _s, _m, norm, manifest = build(archive, tmp_path)
    p = DD.load_panel(tmp_path, manifest, {"symbols": norm["symbols"]}, first_day="2020-01-01", last_day="2020-02-29")
    assert p.shape == (60, 3) and p.symbols == ["AAAUSDTSETTLED", "BTCUSDT", "LUNAUSDT"]
    btc, luna = p.symbols.index("BTCUSDT"), p.symbols.index("LUNAUSDT")
    assert p.close[0, btc] == 10.0 and p.close[30, btc] == 40.0 and p.close[31, btc] == 50.0
    assert not np.isnan(p.close[:, btc]).any()
    assert np.isnan(p.close[20:, luna]).all() and not np.isnan(p.close[:20, luna]).any()   # ended: never carried forward
    assert p.funding_midnight[0, btc] == pytest.approx(-0.0002)
    assert p.funding_later_positive[0, btc] == pytest.approx(0.0002) and p.funding_later_negative[0, btc] == 0.0
    assert p.funding_hours[0, btc] == 24.0 and p.funding_hours[31, btc] == 0.0 and p.funding_hours[10, luna] == 0.0
    early = p.truncated(DD.day_index("2020-01-15"))
    assert early.shape == (15, 3) and early.days[-1] == DD.day_index("2020-01-15")
    window = DD.load_panel(tmp_path, manifest, {"symbols": norm["symbols"]}, first_day="2020-01-10", last_day="2020-01-12")
    assert window.shape == (3, 3) and window.close[0, btc] == 19.0


# ============================== DAILY-FILE COMPLETION AND NON-TRADING BARS ==============================
def test_days_the_monthly_file_skips_come_from_the_archives_daily_files_and_are_counted(archive, tmp_path):
    days = [D2020 + i for i in range(31) if i not in (10, 11, 20)]                 # the monthly file skips three days
    archive.klines("GAPUSDT", "2020-01", [kline_line(d, c=10.0 + (d - D2020), h=11.0 + (d - D2020)) for d in days])
    for i in (10, 11):                                                              # the daily files have two of them
        key = f"{DD.DAILY_KLINES_ROOT}GAPUSDT/1d/GAPUSDT-1d-{DD.day_text(D2020 + i)}.zip"
        archive.add(key, zip_bytes("d.csv", [kline_line(D2020 + i, c=10.0 + i, h=11.0 + i)]))
    disc = DD.discover(get=archive.get, workers=2, first_month="2020-01", last_month="2020-02")
    sync = DD.mirror(tmp_path, disc, get=archive.get, workers=2)
    meta = DD.metadata_snapshot(get=archive.get, fetched_at="t")
    first = DD.normalize(tmp_path, disc, sync, meta)
    assert first["symbols"]["GAPUSDT"]["missing_candles"] == 3 and first["missing_days"]["GAPUSDT"] == [D2020 + 10, D2020 + 11, D2020 + 20]
    done = DD.complete_from_daily_files(tmp_path, disc, first["missing_days"], get=archive.get, workers=2)
    assert done == {"days_requested": 3, "days_found": 2, "days_not_in_archive": 1, "failed": []}
    second = DD.normalize(tmp_path, disc, DD.mirror(tmp_path, disc, get=archive.get, workers=2), meta)
    gap = second["symbols"]["GAPUSDT"]
    assert gap["missing_candles"] == 1 and gap["missing_ranges"] == [["2020-01-21", "2020-01-21"]]   # never invented
    assert gap["candles_from_daily_files"] == 2 and gap["actual_candle_count"] == 30
    manifest = DD.build_manifest(second, disc, meta)
    assert manifest["daily_completion"]["days_found"] == 2 and manifest["quality_totals"]["candles_from_daily_files"] == 2
    assert manifest["raw_artifact_hashes"]["files"] == len(second["inventory"]) == 6 + 1 + 2      # daily files are inventoried
    p = DD.load_panel(tmp_path, manifest, {"symbols": second["symbols"]}, first_day="2020-01-01", last_day="2020-01-31")
    g = p.symbols.index("GAPUSDT")
    assert p.close[10, g] == 20.0 and p.close[11, g] == 21.0 and np.isnan(p.close[20, g])
    # a rebuild from the recorded plan takes exactly the same files
    again = DD.normalize(tmp_path, json.loads(json.dumps(disc)), DD.mirror(tmp_path, disc, get=archive.get, workers=2), meta)
    assert DD.build_manifest(again, disc, meta)["dataset_hash"] == manifest["dataset_hash"]


def test_bars_without_a_trade_are_kept_in_the_table_and_never_used_as_prices(archive, tmp_path):
    lines = [kline_line(D2020 + i, c=10.0 + i, h=11.0 + i) for i in range(12)]
    lines += [kline_line(D2020 + i, o=21.0, h=21.0, l=21.0, c=21.0, v=0.0, qv=0.0, n=0) for i in range(12, 20)]   # settled
    lines += [kline_line(D2020 + i, c=5.0, h=6.0, l=4.0, o=5.0) for i in range(20, 26)]                            # listed again
    lines += [kline_line(D2020 + i, o=5.0, h=5.0, l=5.0, c=5.0, v=0.0, qv=0.0, n=0) for i in range(26, 31)]        # settled again
    archive.klines("ZOMUSDT", "2020-01", lines)
    _d, _s, _m, norm, manifest = build(archive, tmp_path)
    z = norm["symbols"]["ZOMUSDT"]
    assert z["actual_candle_count"] == 31 and z["non_trading_bars"] == 13 and z["missing_candles"] == 0
    assert z["last_traded_day"] == "2020-01-26" and z["non_trading_bars_after_last_trade"] == 5
    assert "ZOMUSDT" in manifest["contracts_that_ended_before_coverage_end"]["symbols"]
    assert manifest["quality_totals"]["non_trading_bars"] == 13
    cov = {"symbols": norm["symbols"]}
    p = DD.load_panel(tmp_path, manifest, cov, first_day="2020-01-01", last_day="2020-01-31")
    j = p.symbols.index("ZOMUSDT")
    assert not np.isnan(p.close[:12, j]).any() and np.isnan(p.close[12:20, j]).all()      # the flat tail is not a price
    assert not np.isnan(p.close[20:26, j]).any() and np.isnan(p.close[26:, j]).all()
    assert np.isnan(p.quote_volume[12:20, j]).all()
    raw = DD.load_panel(tmp_path, manifest, cov, first_day="2020-01-01", last_day="2020-01-31", traded_only=False)
    assert raw.close[15, j] == 21.0                                                        # still in the table
    run = 0
    for value in p.close[:26, j]:                                                          # consecutive real bars up to day 25
        run = 0 if np.isnan(value) else run + 1
    assert run == 6                                                                        # history restarts after the gap
