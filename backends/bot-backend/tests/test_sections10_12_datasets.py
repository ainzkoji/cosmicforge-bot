"""Sections 10-12 dataset program: FX period-verified scale + full-history QA (incl. the August-2024
EURCNH / EURZAR 10x regression), controlled repair with lineage, resumable 1m acquisition, deterministic
bid/ask resampling, gap / session / DST classification, frozen FX and deep-crypto universes, young-symbol
handling, deep acquisition + 5m derivation, supplemental features, coverage determinism, and the frozen v1 /
Section 22 identity guarantees. Synthetic fixtures only (unit tests); dataset evidence comes from the real DBs."""
from __future__ import annotations

import importlib.util
import json
import lzma
import struct
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from shared_lib.persistence.db import DB
from shared_lib.persistence.market_data_schema import ensure_market_data_schema

from app.market_data import fx_reference as fx
from app.market_data import fx_scale, gaps
from app.market_data.store import MarketDataStore, SeriesId

BACKEND = Path(__file__).resolve().parents[1]
REPO = BACKEND.parents[1]
H, MIN, DAY = 3_600_000, 60_000, 86_400_000


def _ms(*a):
    return int(datetime(*a, tzinfo=timezone.utc).timestamp() * 1000)


def _script(name):
    spec = importlib.util.spec_from_file_location(f"{name}_t", REPO / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def db(tmp_path):
    d = DB(path=str(tmp_path / "ds.db"))
    ensure_market_data_schema(d)
    return d


def _bi5(prices, *, start_sec=0, step_sec=3600, point=1e5, vol=10.0):
    raw = b"".join(struct.pack(">IIIIIf", start_sec + i * step_sec, int(round(p * point)), int(round(p * point)),
                               int(round(p * point * 0.999)), int(round(p * point * 1.001)), vol)
                   for i, p in enumerate(prices))
    return lzma.compress(raw)


def _hourly(store, pair, level, start, hours, *, bump=1.0):
    base, quote = pair[:3], pair[3:]
    rows = []
    for i in range(hours):
        v = level * bump
        rows.append({"open_time": start + i * H, "bid_open": v, "bid_high": v * 1.0002, "bid_low": v * 0.9998,
                     "bid_close": v, "ask_open": v * 1.0001, "ask_high": v * 1.0003, "ask_low": v * 0.9999,
                     "ask_close": v * 1.0001, "volume": 1.0})
    store.write_fx_quotes("dukascopy", pair, base, quote, "1h", rows, source_version="t")


# ═══════════════════════════ SECTION 12: scale / QA ═══════════════════════════

def test_infer_point_catches_the_august_2024_provider_point_change():
    # EURCNH 2024-08 raw ints (six decimals) and 2024-09 (five), both near the triangular level 7.87
    assert fx_scale.infer_point([7861043, 7861100], 7.868)[0] == 1e6
    assert fx_scale.infer_point([786027, 786100], 7.868)[0] == 1e5
    assert fx_scale.infer_point([19834270], 19.864)[0] == 1e6          # EURZAR-style
    assert fx_scale.infer_point([108538], 1.0854)[0] == 1e5             # a normal EURUSD file
    p, ev = fx_scale.infer_point([7861043], None)
    assert p is None and ev["reason"] == "NO_INDEPENDENT_REFERENCE" or ev["reason"] == "NO_INDEPENDENT_LEVEL"
    p, ev = fx_scale.infer_point([123], 7.868)
    assert p is None and ev["reason"] == "NO_SCALE_MATCHES_EXPECTED_LEVEL"  # quarantined, never guessed


@pytest.mark.parametrize("factor", [10, 0.1, 100, 0.01])
def test_scale_breaks_flag_power_of_ten_periods_not_market_moves(factor):
    levels = {f"2025-{m:02d}": 7.9 + 0.02 * m for m in range(1, 13)}
    levels["2025-06"] *= factor
    br = fx_scale.scale_breaks(levels)
    assert list(br) == ["2025-06"] and br["2025-06"]["factor"] == pytest.approx(factor)
    # a genuine 60% depreciation over a year (e.g. TRY) is not a scale break
    trend = {f"2025-{m:02d}": 30.0 * (1 + 0.05 * m) for m in range(1, 13)}
    assert fx_scale.scale_breaks(trend) == {}


def test_full_history_cross_rate_qa_reports_the_corrupt_period_and_its_timestamp(db):
    store = MarketDataStore(db)
    jul, aug, sep = _ms(2024, 7, 1), _ms(2024, 8, 1), _ms(2024, 9, 1)
    for start in (jul, aug, sep):
        _hourly(store, "EURUSD", 1.09, start, 48)
        _hourly(store, "USDZAR", 18.2, start, 48)
        _hourly(store, "EURZAR", 1.09 * 18.2, start, 48, bump=10.0 if start == aug else 1.0)
    with db.connect() as conn:
        rel = fx_scale.cross_rate_history(conn, provider="dukascopy", timeframe="1h",
                                          pairs=["EURUSD", "USDZAR", "EURZAR"])
        rep = fx_scale.scale_report(conn, provider="dukascopy", timeframe="1h", pairs=["EURUSD", "USDZAR", "EURZAR"])
    (r,) = rel
    assert r.aligned == 144 and r.failing_periods == ["2024-08"] and not r.passed
    assert aug <= r.max_at_ms < sep and r.max_rel == pytest.approx(9.0, rel=1e-3)
    assert r.to_dict()["max_rel_error_at"].startswith("2024-08")
    assert rep["failing_pairs"] == ["EURZAR"] and rep["pairs"]["EURUSD"]["status"] == "VERIFIED_CONTINUITY"
    # the latest-bar-only check the old validator used would have passed
    assert r.period_medians["2024-09"] < 1e-3


# ── controlled ingestion / repair / resume ────────────────────────────────────

class FakeFetcher:
    def __init__(self, files):
        self.files, self.requests, self.max_requests = files, 0, None

    def budget_left(self):
        return self.max_requests is None or self.requests < self.max_requests

    def get(self, url):
        self.requests += 1
        v = self.files.get(url)
        if isinstance(v, Exception):
            raise v
        return v


def _legs(db, start, n=24 * 31):
    store = MarketDataStore(db)
    _hourly(store, "EURUSD", 1.1, start, n)
    _hourly(store, "USDZAR", 18.0, start, n)


def _avail(db, pairs):
    with db.connect() as c:
        c.execute("CREATE TABLE IF NOT EXISTS fx_reference_availability (provider TEXT, pair TEXT, available INTEGER,"
                  " reason TEXT, probed_period TEXT, recorded_at INTEGER, PRIMARY KEY (provider, pair))")
        for p in pairs:
            c.execute("INSERT OR REPLACE INTO fx_reference_availability VALUES ('dukascopy',?,1,NULL,'t',0)", (p,))


def test_hour_ingest_verifies_scale_per_file_quarantines_the_unverifiable_and_repairs_with_lineage(db):
    s = _script("acquire_fx_reference_dataset")
    aug = _ms(2024, 8, 1)
    _legs(db, aug)
    _avail(db, ["EURUSD", "USDZAR", "EURZAR"])
    url = lambda side: fx.DUKASCOPY_HOUR_URL.format(pair="EURZAR", y=2024, m=7, side=side)  # noqa: E731
    six = {url("BID"): _bi5([19.8] * 24, point=1e6), url("ASK"): _bi5([19.81] * 24, point=1e6)}
    store = MarketDataStore(db)
    res = s._hour_period(db, FakeFetcher(six), store, "EURZAR", 2024, 8, ["EURUSD", "USDZAR", "EURZAR"])
    assert res["ok"] and res["written"] == 24 and res["evidence"]["BID"]["point"] == 1e6
    with db.connect() as c:
        mids = [r[0] for r in c.execute("SELECT mid_close FROM fx_reference_quotes WHERE pair='EURZAR'")]
        log = c.execute("SELECT status, reason FROM fx_reference_ingest_log WHERE pair='EURZAR'").fetchall()
    assert all(19 < m < 21 for m in mids)  # decoded at the verified scale, never 198
    assert all(st == "FETCHED" and "point=1e+06" in why for st, why in log)
    # a file whose scale matches no candidate is quarantined: nothing written, both sides retryable
    bad = {url("BID"): _bi5([0.0003] * 24), url("ASK"): _bi5([0.0003] * 24)}
    with db.connect() as c:
        c.execute("DELETE FROM fx_reference_quotes WHERE pair='EURZAR'")
        c.execute("DELETE FROM fx_reference_ingest_log WHERE pair='EURZAR'")
    res = s._hour_period(db, FakeFetcher(bad), store, "EURZAR", 2024, 8, ["EURUSD", "USDZAR", "EURZAR"])
    assert not res["ok"]
    with db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM fx_reference_quotes WHERE pair='EURZAR'").fetchone()[0] == 0
        assert {r[0] for r in c.execute("SELECT status FROM fx_reference_ingest_log WHERE pair='EURZAR'")} == {"FAILED"}
    # a period stored 10x wrong + quarantined -> controlled replace: delete + insert + lineage, one transaction
    corrupt = [{"open_time": aug + i * H, "bid_close": 198.0, "ask_close": 198.1, "bid_open": 198.0,
                "bid_high": 198.2, "bid_low": 197.9, "ask_open": 198.1, "ask_high": 198.3, "ask_low": 198.0}
               for i in range(24)]
    store.write_fx_quotes("dukascopy", "EURZAR", "EUR", "ZAR", "1h", corrupt, source_version="t")
    res = s._hour_period(db, FakeFetcher(six), store, "EURZAR", 2024, 8, ["EURUSD", "USDZAR", "EURZAR"],
                         replace=True, repair_reason="SCALE_BREAK_PROVIDER_POINT_CHANGE")
    assert res["removed"] == 24 and res["written"] == 24
    with db.connect() as c:
        rep = c.execute("SELECT reason, rows_removed, rows_inserted, validation_json, source_evidence_json FROM "
                        "fx_reference_repairs").fetchone()
        assert max(r[0] for r in c.execute("SELECT mid_close FROM fx_reference_quotes WHERE pair='EURZAR'")) < 21
        assert all(w.startswith("REPAIRED:") for (w,) in c.execute("SELECT reason FROM fx_reference_ingest_log "
                                                                    "WHERE pair='EURZAR'"))
        with pytest.raises(Exception):  # repair lineage is append-only
            c.execute("DELETE FROM fx_reference_repairs")
    assert rep[0] == "SCALE_BREAK_PROVIDER_POINT_CHANGE" and rep[1:3] == (24, 24)
    assert json.loads(rep[3])["passed"] is True and "sha256" in json.loads(rep[4])["BID"]


def test_store_quality_rejection_fails_the_period_and_rolls_back_a_repair(db):
    """Section 13.4: a period the store rejects is recorded FAILED (retryable) -- the worker never crashes, and a
    controlled replace is rolled back as a whole (the old rows are not deleted without their replacement)."""
    s = _script("acquire_fx_reference_dataset")
    aug = _ms(2024, 8, 1)
    _legs(db, aug)
    _avail(db, ["EURUSD", "USDZAR", "EURZAR"])
    url = lambda side: fx.DUKASCOPY_HOUR_URL.format(pair="EURZAR", y=2024, m=7, side=side)  # noqa: E731
    six = {url("BID"): _bi5([19.8] * 24, point=1e6), url("ASK"): _bi5([19.81] * 24, point=1e6)}
    store = MarketDataStore(db)
    old = [{"open_time": aug + i * H, **{f"bid_{k}": 19.8 for k in ("open", "high", "low", "close")},
            **{f"ask_{k}": 19.81 for k in ("open", "high", "low", "close")}} for i in range(24)]
    store.write_fx_quotes("dukascopy", "EURZAR", "EUR", "ZAR", "1h", old, source_version="t")

    def reject(*a, **k):
        raise ValueError("FX_QUOTE_QUALITY_REJECTED")

    store.write_fx_quotes = reject
    res = s._hour_period(db, FakeFetcher(six), store, "EURZAR", 2024, 8, ["EURUSD", "USDZAR", "EURZAR"],
                         replace=True, repair_reason="QUARANTINED_PERIOD_REINGEST")
    assert not res["ok"] and res["written"] == res["removed"] == 0
    with db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM fx_reference_quotes WHERE pair='EURZAR'").fetchone()[0] == 24
        assert c.execute("SELECT COUNT(*) FROM fx_reference_repairs").fetchone()[0] == 0
        log = c.execute("SELECT status, reason FROM fx_reference_ingest_log WHERE pair='EURZAR'").fetchall()
    assert {st for st, _ in log} == {"FAILED"} and all("QUALITY_REJECTED" in why for _, why in log)


def test_minute_acquisition_is_resumable_bounded_and_marks_saturdays(db):
    s = _script("acquire_fx_reference_dataset")
    _avail(db, ["EURUSD"])
    store = MarketDataStore(db)
    _hourly(store, "EURUSD", 1.1, _ms(2024, 8, 1), 24 * 31)
    days = [datetime(2024, 8, d).date() for d in (2, 3, 5)]  # Fri, Sat, Mon

    def files(d):
        start = 0
        return {fx.DUKASCOPY_URL.format(pair="EURUSD", y=2024, m=7, d=d.day, side=side):
                _bi5([1.1] * 3, start_sec=start, step_sec=60) for side in ("BID", "ASK")}

    f = FakeFetcher({k: v for d in days for k, v in files(d).items()})
    out = s.acquire_minutes(db, f, ["EURUSD"], days[0], days[-1], available=["EURUSD"])
    assert out["pairs"]["EURUSD"]["fetched"] == 3 and out["pairs"]["EURUSD"]["saturdays_marked"] == 1
    assert f.requests == 6  # Fri, Sun (provider has no file -> NO_FILE) and Mon; Saturday is never requested
    with db.connect() as c:
        log = dict(c.execute("SELECT period || side, status FROM fx_reference_ingest_log WHERE timeframe='1m'"))
    assert log["2024-08-03BID"] == "EMPTY" and log["2024-08-04BID"] == "NO_FILE"
    again = FakeFetcher({})
    out = s.acquire_minutes(db, again, ["EURUSD"], days[0], days[-1], available=["EURUSD"])
    assert again.requests == 0 and out["pairs"]["EURUSD"]["already_done"] == 4  # resume: nothing refetched
    with db.connect() as c:
        n = c.execute("SELECT COUNT(*) FROM fx_reference_quotes WHERE timeframe='1m'").fetchone()[0]
        c.execute("UPDATE fx_reference_ingest_log SET status='FAILED' WHERE period='2024-08-05'")
    retry = FakeFetcher(files(days[2]))
    s.acquire_minutes(db, retry, ["EURUSD"], days[0], days[-1], available=["EURUSD"])
    with db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM fx_reference_quotes WHERE timeframe='1m'").fetchone()[0] == n  # idempotent
    assert retry.requests == 2  # only the FAILED period is fetched again
    budget = FakeFetcher({})
    budget.max_requests = 0
    with db.connect() as c:
        c.execute("DELETE FROM fx_reference_ingest_log WHERE period='2024-08-05'")
    assert s.acquire_minutes(db, budget, ["EURUSD"], days[0], days[-1], available=["EURUSD"])["stopped"] == \
        "BUDGET_EXHAUSTED"


def test_provider_refusals_stop_the_run_instead_of_spinning(db):
    s = _script("acquire_fx_reference_dataset")
    _avail(db, ["EURUSD"])

    class Down(FakeFetcher):
        def get(self, url):
            raise s.ProviderUnavailable("reset")

    d = datetime(2024, 8, 5).date()
    out = s.acquire_minutes(db, Down({}), ["EURUSD"], d, d, available=["EURUSD"])
    assert out["stopped"].startswith("PROVIDER_UNAVAILABLE")


# ── resampling ─────────────────────────────────────────────────────────────────

def _minutes(start, n, bid=1.1):
    return [{"open_time": start + i * MIN, "bid_open": bid + i * 1e-5, "bid_high": bid + i * 1e-5 + 2e-5,
             "bid_low": bid + i * 1e-5 - 2e-5, "bid_close": bid + (i + 1) * 1e-5,
             "ask_open": bid + i * 1e-5 + 1e-4, "ask_high": bid + i * 1e-5 + 1.3e-4,
             "ask_low": bid + i * 1e-5 + 0.8e-4, "ask_close": bid + (i + 1) * 1e-5 + 1e-4, "volume": 1.0}
            for i in range(n)]


@pytest.mark.parametrize("factor", [5, 15, 60, 240])
def test_bid_and_ask_resample_separately_and_deterministically(factor):
    q = _minutes(_ms(2025, 3, 10), 480)
    out = fx.resample_quotes(q, factor)
    assert len(out) == 480 // factor and out == fx.resample_quotes(q, factor)
    with pytest.raises(ValueError, match="INVALID_SOURCE"):
        fx.resample_quotes(list(reversed(q)), factor)
    first = q[:factor]
    b = out[0]
    assert b["bid_open"] == first[0]["bid_open"] and b["bid_close"] == first[-1]["bid_close"]
    assert b["bid_high"] == max(x["bid_high"] for x in first) and b["ask_low"] == min(x["ask_low"] for x in first)
    assert b["mid_close"] == pytest.approx((b["bid_close"] + b["ask_close"]) / 2)
    # an incomplete window is dropped, never approximated; nothing finer is ever fabricated
    assert fx.resample_quotes(q[: factor - 1], factor) == []


def test_derive_command_writes_5m_15m_4h_from_real_1m_with_source_lineage(db):
    s = _script("acquire_fx_reference_dataset")
    store = MarketDataStore(db)
    store.write_fx_quotes("dukascopy", "EURUSD", "EUR", "USD", "1m", _minutes(_ms(2025, 3, 10), 480),
                          source_version="bi5-candles-min-1:v1")
    out = s.derive(db, "EURUSD")
    assert out == {"5m": 96, "15m": 32, "4h": 2}
    assert s.derive(db, "EURUSD") == {"5m": 0, "15m": 0, "4h": 0}  # no newly inserted rows
    with db.connect() as c:
        tags = dict(c.execute("SELECT timeframe, source_version FROM fx_reference_quotes GROUP BY timeframe"))
        assert c.execute("SELECT COUNT(*) FROM fx_reference_quotes WHERE timeframe='4h'").fetchone()[0] == 2
    assert tags["4h"] == "derived-from-1m:240:v1" and tags["5m"] == "derived-from-1m:5:v1"


def test_missing_side_is_never_zero_and_mid_spread_are_explicit_derivations(db):
    merged = fx.merge_sides([{"open_time": 0, "open": 1.1, "high": 1.2, "low": 1.0, "close": 1.1, "volume": 1}], [],
                            pair="EURUSD")
    assert merged[0]["ask_close"] is None and "mid_close" not in merged[0]
    store = MarketDataStore(db)
    store.write_fx_quotes("dukascopy", "EURUSD", "EUR", "USD", "1m", merged, source_version="t")
    store.write_fx_quotes("dukascopy", "GBPUSD", "GBP", "USD", "1m",
                          [{"open_time": 0, **{f"bid_{f}": 1.25 for f in ("open", "high", "low", "close")},
                            **{f"ask_{f}": 1.2502 for f in ("open", "high", "low", "close")}}], source_version="t")
    with db.connect() as c:
        e = c.execute("SELECT bid_close, ask_close, mid_close, spread_close, price_kind FROM fx_reference_quotes "
                      "WHERE pair='EURUSD'").fetchone()
        g = c.execute("SELECT mid_close, spread_close FROM fx_reference_quotes WHERE pair='GBPUSD'").fetchone()
    assert e[1] is None and e[2] is None and e[3] is None and e[4] == "REFERENCE_MARKET_PRICE"
    assert g[0] == pytest.approx(1.2501) and g[1] == pytest.approx(0.0002)


# ── gaps / sessions / DST ───────────────────────────────────────────────────────

def test_fx_weekly_close_follows_new_york_time_across_dst():
    assert not gaps.fx_weekend_closed(_ms(2025, 1, 10, 21, 30))   # Fri 16:30 EST: still open
    assert gaps.fx_weekend_closed(_ms(2025, 1, 10, 22, 30))       # Fri 17:30 EST: closed
    assert gaps.fx_weekend_closed(_ms(2025, 7, 11, 21, 30))       # Fri 17:30 EDT: closed
    assert gaps.fx_weekend_closed(_ms(2025, 7, 13, 20, 30)) and not gaps.fx_weekend_closed(_ms(2025, 7, 13, 21, 30))
    # the US DST switch (2025-03-09) neither duplicates nor removes a UTC hour
    week = gaps.fx_expected_bars(_ms(2025, 3, 9), _ms(2025, 3, 16), H)
    assert week == gaps.fx_expected_bars(_ms(2025, 3, 16), _ms(2025, 3, 23), H)


def test_fx_gap_classification():
    fri_close, sun_open = _ms(2025, 7, 11, 21), _ms(2025, 7, 13, 21)
    assert gaps.classify_fx_gap(fri_close, sun_open - H, H) == gaps.WEEKEND_CLOSED
    assert gaps.classify_fx_gap(_ms(2025, 12, 25, 10), _ms(2025, 12, 25, 12), H) == gaps.HOLIDAY_CLOSED
    assert gaps.classify_fx_gap(_ms(2025, 7, 15, 21, 0), _ms(2025, 7, 15, 21, 10), MIN) == gaps.SESSION_CLOSED
    tue = _ms(2025, 7, 15, 10)
    assert gaps.classify_fx_gap(tue, tue + 2 * H, H, ingest_status={"2025-07-15": "NO_FILE"}) == gaps.PROVIDER_NO_FILE
    assert gaps.classify_fx_gap(tue, tue + 2 * H, H, ingest_status={"2025-07-15": "FAILED"}) == gaps.INGEST_FAILURE
    assert gaps.classify_fx_gap(tue, tue + 2 * H, H, ingest_status={"2025-07-15": "FETCHED"}) == gaps.UNKNOWN_GAP
    assert gaps.classify_fx_gap(tue, tue + 2 * H, H, ingest_status={"2025-07": "NO_FILE"},
                                period_kind="month") == gaps.PROVIDER_NO_FILE


def test_gap_detection_and_crypto_classification():
    t = [0, MIN, 2 * MIN, 5 * MIN]
    assert gaps.find_gaps(t, MIN, start_ms=0, end_ms=7 * MIN) == [(3 * MIN, 4 * MIN, 2), (6 * MIN, 6 * MIN, 1)]
    assert gaps.classify_crypto_gap(0, 10, listed_at_ms=100) == gaps.LISTING_AGE
    assert gaps.classify_crypto_gap(200, 300, listed_at_ms=100, failed_periods=[(150, 400)]) == gaps.PROVIDER_FAILURE
    assert gaps.classify_crypto_gap(200, 300, evidence={gaps.VENUE_OUTAGE: [(100, 400)]}) == gaps.VENUE_OUTAGE
    assert gaps.classify_crypto_gap(200, 300, listed_at_ms=100) == gaps.UNKNOWN_GAP  # no evidence -> unknown
    s = gaps.summarize([gaps.Gap(0, 1, 2, gaps.WEEKEND_CLOSED), gaps.Gap(5, 6, 1, gaps.UNKNOWN_GAP)])
    assert s["unexpected_gaps"] == 1


# ── FX universe freeze ──────────────────────────────────────────────────────────

def _fx_manifest(**kw):
    from app.market_data.fx_universe import build_fx_universe_manifest

    members = kw.pop("members", [{"pair": "EURUSD", "base": "EUR", "quote": "USD", "scale_status": "VERIFIED_CONTINUITY",
                                  "cross_rate_status": "NO_RELATION_USD_LEG"}])
    return build_fx_universe_manifest(provider="dukascopy", members=members, excluded={"USDHUF": "UNVALIDATED"},
                                      window_start_ms=0, window_end_ms=DAY, scale_qa_version="v", gap_policy_version="g",
                                      source_versions=["a"], generated_at=kw.get("at", "x"), code_commit=None)


def test_fx_universe_is_frozen_independently_and_refuses_edits_and_scale_breaks():
    from app.market_data.fx_universe import FrozenFxUniverseError, load_fx_universe, verify_fx_universe

    a, b = _fx_manifest(at="t1"), _fx_manifest(at="t2")
    assert a["universe_hash"] == b["universe_hash"] and a["holdout_opened"] is False
    assert a["execution_authorized"] is False and a["price_kind"] == "REFERENCE_MARKET_PRICE"
    edited = {**a, "members": a["members"] + [{"pair": "GBPUSD"}]}
    with pytest.raises(FrozenFxUniverseError):
        verify_fx_universe(edited)
    with pytest.raises(FrozenFxUniverseError):
        verify_fx_universe({**a, "holdout_opened": True})
    with pytest.raises(FrozenFxUniverseError):
        _fx_manifest(members=[{"pair": "EURZAR", "base": "EUR", "quote": "ZAR", "scale_status": "SCALE_BREAK",
                               "cross_rate_status": "FAIL"}])
    committed = load_fx_universe(str(REPO / "docs/research/cati_fx_universe_dukascopy_v1.json"))
    assert len(committed["members"]) == 50 and len(committed["excluded"]) == 12
    assert committed["universe_hash"] != json.load(open(REPO / "docs/research/cati_crypto_universe_binance_v1.json",
                                                        encoding="utf-8"))["universe_hash"]
    assert all(v == "UNVALIDATED_PRICE_SCALE" for v in committed["excluded"].values())


# ═══════════════════════════ SECTION 11: crypto ═══════════════════════════════

def _ins(sym, listed, *, status=True):
    return SimpleNamespace(venue_symbol=sym, asset_class="CRYPTO", product_type="PERPETUAL", settlement_asset="USDT",
                           api_tradable=status, listed_at_ms=listed, canonical_symbol=f"{sym[:-4]}/USDT:PERP",
                           base_currency=sym[:-4], quote_currency="USDT", venue="binance_usdm")


def test_young_symbols_are_included_as_insufficient_history_only_under_the_new_policy():
    from app.market_data.universe import HistoricalLiquidity, SelectionCriteria, build_frozen_universe_manifest, \
        select_universe

    now = 1000 * DAY
    ins = [_ins("OLDUSDT", 0), _ins("NEWUSDT", now - 100 * DAY), _ins("FUTUREUSDT", now + DAY),
           _ins("DEADUSDT", 0, status=False)]
    stats = {i.venue_symbol: HistoricalLiquidity(5e6) for i in ins}
    v1 = select_universe(ins, stats, venue="binance_usdm", as_of_ms=now,
                         criteria=SelectionCriteria(min_quote_volume_24h=2e6, max_spread_bps=None, min_size=1))
    assert v1.selected == ("OLDUSDT",) and v1.excluded["NEWUSDT"] == "LISTING_TOO_RECENT"
    assert "young_symbol_policy" not in v1.criteria and not v1.history_status  # v1 bytes unchanged
    crit = SelectionCriteria(min_quote_volume_24h=2e6, max_spread_bps=None, min_size=1,
                             young_symbol_policy="INCLUDE_INSUFFICIENT_HISTORY")
    v2 = select_universe(ins, stats, venue="binance_usdm", as_of_ms=now, criteria=crit)
    assert set(v2.selected) == {"OLDUSDT", "NEWUSDT"} and v2.history_status == {"NEWUSDT": "INSUFFICIENT_HISTORY"}
    assert v2.excluded["FUTUREUSDT"] == "LISTED_AFTER_SELECTION_CUTOFF" and v2.excluded["DEADUSDT"] == "NOT_TRADING"
    man = build_frozen_universe_manifest(v2, ins, window_start_ms=now - 731 * DAY, window_end_ms=now,
                                         timeframes=["15m"], source_provider="binance", generated_at="x")
    young = next(m for m in man["members"] if m["venue_symbol"] == "NEWUSDT")
    assert young["history_status"] == "INSUFFICIENT_HISTORY" and young["requested_start_ms"] == now - 100 * DAY
    assert v2.manifest_hash != select_universe(ins, stats, venue="binance_usdm", as_of_ms=now,
                                               criteria=SelectionCriteria(min_quote_volume_24h=2e6,
                                                                          max_spread_bps=None, min_size=1)).manifest_hash


def test_frozen_v1_crypto_universe_is_unchanged():
    from app.market_data.universe import load_frozen_universe

    v1 = load_frozen_universe(str(REPO / "docs/research/cati_crypto_universe_binance_v1.json"))
    assert v1["universe_hash"].startswith("a5b5d1ee") and len(v1["members"]) == 136
    assert all("history_status" not in m for m in v1["members"])


def test_deep_universe_is_deterministic_historical_and_versioned():
    from app.market_data.universe import (FrozenUniverseError, build_deep_universe_manifest, load_deep_universe,
                                          load_frozen_universe, rank_deep_subset, verify_deep_universe)

    parent = load_frozen_universe(str(REPO / "docs/research/cati_crypto_universe_binance_v1.json"))
    syms = [m["venue_symbol"] for m in parent["members"]]
    scores = {s: float(1000 - i) for i, s in enumerate(syms)}
    scores[syms[5]] = scores[syms[4]]  # a tie -> symbol order decides
    scores[syms[0]] = None             # unknown liquidity is never ranked
    kw = dict(days_observed={s: 365 for s in syms}, liquidity_window_start_ms=0, liquidity_window_end_ms=1,
              liquidity_source="t", code_commit=None)
    a = build_deep_universe_manifest(parent, scores, generated_at="1", **kw)
    b = build_deep_universe_manifest(parent, dict(reversed(list(scores.items()))), generated_at="2", **kw)
    assert a["universe_hash"] == b["universe_hash"] and len(a["members"]) == 35 and syms[0] not in \
        [m["venue_symbol"] for m in a["members"]]
    tie = [x for x, _ in rank_deep_subset({"BBB": 1.0, "AAA": 1.0})]
    assert tie == ["AAA", "BBB"]
    changed = dict(scores)
    changed[syms[1]] = 0.0
    assert build_deep_universe_manifest(parent, changed, generated_at="1", **kw)["universe_hash"] != a["universe_hash"]
    with pytest.raises(FrozenUniverseError):
        verify_deep_universe({**a, "size": 36})
    with pytest.raises(ValueError):
        build_deep_universe_manifest(parent, scores, generated_at="1", size=45, **kw)
    committed = load_deep_universe(str(REPO / "docs/research/cati_crypto_deep_universe_binance_v1.json"))
    assert committed["parent_universe_hash"] == parent["universe_hash"] and len(committed["members"]) == 35
    assert committed["role"] == "RESEARCH_UNIVERSE" and committed["execution_authorized"] is False
    assert [m["rank"] for m in committed["members"]] == list(range(1, 36))
    for m in committed["members"]:  # listing-limited members start at their listing, never earlier
        target = committed["window_end_ms"] - committed["target_history_days"] * DAY
        assert m["requested_start_ms"] == max(target, m["listed_at_ms"])
        assert (m["history_status"] == "INSUFFICIENT_HISTORY") == (m["listed_at_ms"] > target)


def test_multi_asset_script_refuses_the_snapshot_deep_path():
    src = (REPO / "scripts" / "acquire_multi_asset_dataset.py").read_text(encoding="utf-8")
    assert "deep_subset(" not in src and "--deep is refused" in src


# ── deep acquisition / 5m / features ────────────────────────────────────────────

def _k(t, o=100.0, h=101.0, low=99.0, c=100.5, v=2.0, qv=200.0, trades=5):
    return [t, str(o), str(h), str(low), str(c), str(v), t + MIN - 1, str(qv), trades]


class FakeApi:
    def __init__(self, klines, fail_periods=()):
        self.klines, self.fail, self.requests, self.max_requests, self.deadline = klines, set(fail_periods), 0, None, None

    def budget_left(self):
        return self.max_requests is None or self.requests < self.max_requests

    def get(self, path, params):
        self.requests += 1
        a, b = params["startTime"], params["endTime"]
        if any(x <= a < y for x, y in self.fail):
            raise ValueError("HTTP 400 simulated")
        return [k for k in self.klines if a <= k[0] <= b][: params["limit"]]


def test_deep_acquisition_listing_start_resume_invalid_rows_and_failures(db):
    s = _script("acquire_crypto_deep_dataset")
    listed = _ms(2024, 3, 15)
    member = {"venue_symbol": "NEWUSDT", "canonical_instrument_id": "NEW/USDT:PERP", "asset_class": "CRYPTO",
              "product_type": "PERPETUAL", "listed_at_ms": listed, "requested_start_ms": _ms(2024, 2, 1)}
    end = _ms(2024, 4, 1)
    kl = [_k(t) for t in range(listed, end, 30 * MIN)]
    kl.append([listed + MIN, "100", "90", "99", "100", "1", listed + 2 * MIN - 1, "1", 1])  # high < low: invalid
    store = MarketDataStore(db)
    api = FakeApi(kl)
    r = s.acquire_member(db, api, member, end, store=store)
    with db.connect() as c:
        log = dict(c.execute("SELECT period, status || ':' || IFNULL(reason,'') FROM market_ingest_log"))
        n = c.execute("SELECT COUNT(*) FROM market_candles").fetchone()[0]
    assert log["2024-02"] == "NOT_LISTED:BEFORE_LISTING" and log["2024-03"].startswith("FETCHED:INVALID_ROWS_REJECTED=1")
    assert n == len(kl) - 1  # no bar before listing, the invalid bar rejected (never repaired), nothing padded
    before = api.requests
    again = s.acquire_member(db, api, member, end, store=store)
    assert again["skipped"] == 2 and again["fetched"] == 0 and api.requests == before  # resume: nothing refetched
    with db.connect() as c:
        c.execute("UPDATE market_ingest_log SET status='FAILED' WHERE period='2024-03'")
    s.acquire_member(db, FakeApi(kl), member, end, store=store)
    with db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM market_candles").fetchone()[0] == n  # idempotent re-fetch
    failing = FakeApi(kl, fail_periods=[(listed, end)])
    with db.connect() as c:
        c.execute("DELETE FROM market_ingest_log")
    s.acquire_member(db, failing, member, end, store=store)
    with db.connect() as c:
        assert c.execute("SELECT status FROM market_ingest_log WHERE period='2024-03'").fetchone()[0] == "FAILED"


def test_5m_derivation_never_fabricates_volume():
    s = _script("acquire_crypto_deep_dataset")
    t0 = _ms(2025, 1, 1)
    rows = [[t0 + i * MIN, "1", "2", "0.5", "1.5", "1", t0 + (i + 1) * MIN - 1, 10.0, 2] for i in range(10)]
    rows[7][7] = None  # a minute without quote volume
    out = s.derive_rows(rows + [[t0 + 12 * MIN, "1", "2", "0.5", "1.5", "1", t0 + 13 * MIN - 1, 1.0, 1]], "5m")
    assert [r[0] for r in out] == [t0, t0 + 5 * MIN]  # the incomplete third window is dropped
    assert out[0][7] == 50.0 and out[0][8] == 10 and out[1][7] is None  # summed only when every minute has it


def test_venue_identity_and_delisted_lineage_are_preserved(db):
    from app.exchange.instruments import InstrumentCatalog, parse_binance_symbol

    store = MarketDataStore(db)
    row = [[_ms(2025, 1, 1), "1", "2", "0.5", "1.5", "1", _ms(2025, 1, 1) + MIN - 1, 1.0, 1]]
    for venue in ("binance_usdm", "bybit_linear"):
        store.write_candles(SeriesId(venue, "BTCUSDT", "BTC/USDT:PERP", "CRYPTO", "PERPETUAL", "k"), "1m", row)
    with db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM market_candles WHERE venue_symbol='BTCUSDT'").fetchone()[0] == 2
    cat = InstrumentCatalog(db)
    old = parse_binance_symbol({"symbol": "OLDUSDT", "baseAsset": "OLD", "quoteAsset": "USDT", "status": "TRADING",
                                "contractType": "PERPETUAL", "underlyingType": "COIN", "filters": []})
    btc = parse_binance_symbol({"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "status": "TRADING",
                                "contractType": "PERPETUAL", "underlyingType": "COIN", "filters": []})
    cat.upsert("binance_usdm", "LIVE", [old, btc], 1)
    store.write_candles(SeriesId("binance_usdm", "OLDUSDT", "OLD/USDT:PERP", "CRYPTO", "PERPETUAL", "k"), "1m", row)
    cat.upsert("binance_usdm", "LIVE", [btc], 2)  # OLDUSDT delisted
    assert cat.record("binance_usdm", "LIVE", "OLDUSDT")["state"] == "DELISTED"
    assert store.read_candles(venue="binance_usdm", venue_symbol="OLDUSDT", timeframe="1m")  # history kept


def test_features_record_unavailable_with_reasons_never_zero(db):
    s = _script("acquire_crypto_deep_dataset")
    member = {"venue_symbol": "BTCUSDT", "canonical_instrument_id": "BTC/USDT:PERP", "asset_class": "CRYPTO",
              "product_type": "PERPETUAL", "listed_at_ms": 0, "requested_start_ms": _ms(2025, 1, 1)}
    end, now = _ms(2025, 1, 3), _ms(2026, 9, 26)

    class Api(FakeApi):
        def get(self, path, params):
            self.requests += 1
            a, b = params.get("startTime"), params.get("endTime")
            # like the venue: only real, grid-aligned records at or after startTime
            if path.endswith("fundingRate"):
                return [{"fundingTime": t, "fundingRate": "0.0001"} for t in range(0, b + 1, 8 * H) if t >= a][:1000]
            if "Klines" in path:
                return [[t, "1", "1", "1", "100.5" if "mark" in path else "100", "0", t + H - 1]
                        for t in range(0, b + 1, H) if t >= a][:1000]
            return []

    store = MarketDataStore(db)
    s.features_member(db, Api([]), member, end, store=store, now_ms=now)
    with db.connect() as c:
        rows = {f: (st, v, why) for f, st, v, why in c.execute(
            "SELECT feature, status, value, unavailable_reason FROM market_feature_observations "
            "WHERE status='UNAVAILABLE'")}
        basis = c.execute("SELECT value FROM market_feature_observations WHERE feature='basis_bps'").fetchall()
        funding = c.execute("SELECT COUNT(*) FROM market_feature_observations WHERE feature='funding_rate'").fetchone()
    assert rows["open_interest"][2].startswith("NO_HISTORICAL_ENDPOINT") and rows["open_interest"][1] is None
    assert rows["spread_bps"][2].startswith("NO_HISTORICAL_ENDPOINT")
    assert rows["liquidations_long"][2].startswith("NOT_SUPPORTED_BY_PROVIDER")
    assert all(v is None for _, v, _ in rows.values())  # missing is never 0
    assert basis and all(v == pytest.approx(50.0) for (v,) in basis) and funding[0] == 6


# ═══════════════════════════ SECTION 10: program / identity ═══════════════════

def test_coverage_artifacts_are_deterministic(tmp_path):
    cov = _script("dataset_coverage")
    a = cov._write(str(tmp_path / "a.json"), {"x": [1, 2], "y": {"b": 1, "a": 2}})
    b = cov._write(str(tmp_path / "b.json"), {"y": {"a": 2, "b": 1}, "x": [1, 2]})
    assert a["content_hash"] == b["content_hash"]
    committed = json.load(open(REPO / "docs/research/coverage/crypto_broad_binance_v1.coverage.json", encoding="utf-8"))
    agg = committed["aggregate"]
    assert agg["reconciles_to_db"] and agg["rows"] == 9_543_936 and agg["symbols"] == 136 and agg["gaps"] == 0
    assert committed["universe_hash"].startswith("a5b5d1ee") and "legacy_limitations" in committed["provenance"]
    body = {k: v for k, v in committed.items() if k not in ("content_hash", "generated_at")}
    import hashlib

    assert hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest() == \
        committed["content_hash"]


def test_schema_additions_are_additive_and_historical_candles_is_untouched(tmp_path):
    import sqlite3

    d = DB(path=str(tmp_path / "old.db"))
    with d.connect() as c:
        before = c.execute("SELECT sql FROM sqlite_master WHERE name='historical_candles'").fetchone()
    ensure_market_data_schema(d)
    ensure_market_data_schema(d)  # idempotent
    with d.connect() as c:
        after = c.execute("SELECT sql FROM sqlite_master WHERE name='historical_candles'").fetchone()
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        c.execute("INSERT INTO market_ingest_log VALUES ('v','S','klines','1m','2025-01','FETCHED',1,NULL,0)")
        with pytest.raises(sqlite3.IntegrityError):
            c.execute("INSERT INTO market_ingest_log VALUES ('v','S','klines','1m','2025-02','BOGUS',1,NULL,0)")
    assert before == after  # the Section 22 canonical table is never altered by the dataset schema
    assert {"fx_reference_repairs", "market_ingest_log"} <= tables
