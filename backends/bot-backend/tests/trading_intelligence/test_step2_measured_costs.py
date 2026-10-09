"""Section H, Step 2.7: measured-cost records, provenance, the cost table and versioned calibration.
Offline: the public endpoints are replaced by an in-memory getter; no order is placed and no key is read."""
from __future__ import annotations

import json
import sqlite3

import pytest

from app.trading_intelligence.research.costs import measured as MC
from app.trading_intelligence.research.evaluator.official import frozen_cost_model


def book(symbol="BTCUSDT", bid=100.0, ask=100.02, day="2026-10-09", n=0, source=MC.LIVE_PUBLIC_OBSERVATION, **kw):
    base = dict(exchange="BINANCE", symbol=symbol, market_type="USDM_PERPETUAL", timestamp_utc=f"{day}T00:00:{n % 60:02d}Z",
                bid_price=bid, ask_price=ask, data_source=source, environment="PUBLIC_MARKET_DATA", measurement_quality="SNAPSHOT",
                notes=str(n))
    base.update(kw)
    return MC.make_record(**base)


def fill(expected=100.0, actual=100.05, side="BUY", source=MC.LIVE_PUBLIC_OBSERVATION, day="2026-10-09", n=0, **kw):
    sign = 1.0 if side == "BUY" else -1.0
    base = dict(exchange="BINANCE", symbol="BTCUSDT", market_type="USDM_PERPETUAL", timestamp_utc=f"{day}T01:00:{n % 60:02d}Z",
                order_side=side, expected_price=expected, actual_fill_price=actual,
                observed_slippage_bps=(actual - expected) / expected * 1e4 * sign, data_source=source, environment="X",
                measurement_quality="Q", notes=str(n))
    base.update(kw)
    return MC.make_record(**base)


def test_a_record_has_every_field_and_derives_the_spread_from_the_book():
    r = book()
    assert set(MC.FIELDS) <= set(r) and r["mid_price"] == pytest.approx(100.01)
    assert r["spread_absolute"] == pytest.approx(0.02) and r["spread_bps"] == pytest.approx(0.02 / 100.01 * 1e4)
    assert r["actual_fill_price"] is None and "actual_fill_price" in r["unavailable"]      # not captured stays None
    assert r["schema"] == MC.MEASURED_COST_SCHEMA and r["record_id"] == book()["record_id"]


def test_invalid_or_invented_values_are_refused():
    with pytest.raises(MC.CostRecordError, match="missing"):
        MC.make_record(exchange="BINANCE", symbol="BTCUSDT")
    with pytest.raises(MC.CostRecordError, match="provenance"):
        book(source="MEASURED")
    with pytest.raises(MC.CostRecordError, match="bid <= ask"):
        book(bid=101.0, ask=100.0)
    with pytest.raises(MC.CostRecordError, match="disagrees"):
        book(spread_bps=0.5)                                       # a spread that is not what the book says
    with pytest.raises(MC.CostRecordError, match="needs the bid and ask"):
        MC.make_record(exchange="BINANCE", symbol="BTCUSDT", market_type="M", timestamp_utc="t", spread_bps=1.0,
                       data_source=MC.LIVE_PUBLIC_OBSERVATION, environment="E", measurement_quality="Q")
    with pytest.raises(MC.CostRecordError, match="slippage needs"):
        MC.make_record(exchange="BINANCE", symbol="BTCUSDT", market_type="M", timestamp_utc="t", observed_slippage_bps=3.0,
                       data_source=MC.DEMO_EXECUTION, environment="DEMO", measurement_quality="Q")
    with pytest.raises(MC.CostRecordError, match="disagrees"):
        fill(observed_slippage_bps=1.0)
    with pytest.raises(MC.CostRecordError, match="finite"):
        book(funding_rate=float("nan"))
    with pytest.raises(MC.CostRecordError, match="unknown cost fields"):
        MC.make_record(guess=1)
    assumed = MC.make_record(exchange="BINANCE", symbol="BTCUSDT", market_type="M", timestamp_utc="t", spread_bps=10.0,
                             data_source=MC.ASSUMED, environment="RESEARCH", measurement_quality="ASSUMPTION")
    assert assumed["data_source"] == MC.ASSUMED and MC.ASSUMED not in MC.MEASUREMENTS      # allowed, labelled, never measured


def test_slippage_is_adverse_positive_on_both_sides():
    assert fill(100.0, 100.05, "BUY")["observed_slippage_bps"] == pytest.approx(5.0)
    assert fill(100.0, 99.95, "SELL")["observed_slippage_bps"] == pytest.approx(5.0)
    assert fill(100.0, 99.95, "BUY")["observed_slippage_bps"] == pytest.approx(-5.0)       # price improvement


def test_the_store_is_append_only_and_keeps_one_copy_of_each_observation(tmp_path):
    store = MC.MeasuredCostStore(tmp_path / "obs.jsonl")
    assert store.append([book(n=1), book(n=2)]) == 2 and store.append([book(n=1), book(n=3)]) == 1
    assert len(store.records()) == 3 and b"\r" not in store.path.read_bytes()
    with pytest.raises(MC.CostRecordError):
        store.append([{**book(n=9), "data_source": "INVENTED"}])
    assert len(store.records()) == 3


def test_public_observation_uses_public_endpoints_only_and_selects_in_scope_symbols():
    calls = []

    def get(url):
        calls.append(url)
        if url == MC.BOOK_TICKER_URL:
            return json.dumps([{"symbol": "BTCUSDT", "bidPrice": "100.0", "askPrice": "100.1"},
                               {"symbol": "ETHUSDT", "bidPrice": "0", "askPrice": "0"},
                               {"symbol": "XRPUSDT", "bidPrice": "1.0", "askPrice": "1.001"}]).encode()
        if url == MC.PREMIUM_INDEX_URL:
            return json.dumps([{"symbol": "BTCUSDT", "lastFundingRate": "0.0001", "nextFundingTime": 1}]).encode()
        return json.dumps([{"symbol": "BTCUSDT", "quoteVolume": "9"}, {"symbol": "USDCUSDT", "quoteVolume": "99"},
                           {"symbol": "ETHUSDT", "quoteVolume": "8"}, {"symbol": "BTCUSDC", "quoteVolume": "50"}]).encode()

    assert MC.most_traded_symbols(get, lambda s: s != "USDCUSDT", count=2) == ["BTCUSDT", "ETHUSDT"]
    recs = MC.observe_public_book(get, ["BTCUSDT", "ETHUSDT"], timestamp_utc="2026-10-09T00:00:00Z")
    assert [r["symbol"] for r in recs] == ["BTCUSDT"]                                       # an empty book is skipped, not faked
    assert recs[0]["data_source"] == MC.LIVE_PUBLIC_OBSERVATION and recs[0]["funding_rate"] == 0.0001
    assert recs[0]["spread_bps"] == pytest.approx(0.1 / 100.05 * 1e4) and recs[0]["environment"] == "PUBLIC_MARKET_DATA"
    assert all(u.startswith("https://fapi.binance.com/fapi/v1/") for u in calls)
    assert not any(k in u for u in calls for k in ("order", "account", "signature", "listenKey"))


def test_demo_fills_are_read_without_identifiers_and_are_never_representative(tmp_path):
    db = tmp_path / "runtime.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE cati_execution_attempts (status TEXT, payload TEXT, recorded_at INTEGER)")
    key = {"venue": "binance_usdm", "venue_symbol": "BTCUSDT"}
    filled = {"environment": "DEMO", "instrument_key": key, "side": "LONG", "requested_price": 100.0, "filled_price": 100.03,
              "filled_quantity": 0.01, "broker_account_id": "acct-secret", "user_id": "u-1", "actual_order_type": "MARKET",
              "submitted_at": 1000, "acknowledged_at": 1000, "realized_costs": {"fees": 0.0005, "slippage_bps": 3.0}}
    rows = [
        ("PENDING_SUBMIT", {**filled, "filled_price": None, "realized_costs": {}}),
        ("FILLED", filled),
        ("POSITION_CLOSED", filled),                                                           # repeats the same fill
        ("FILLED", {"environment": "DEMO", "instrument_key": {"venue": "binance_usdm", "venue_symbol": "ETHUSDT"}, "side": "LONG",
                    "realized_costs": {"fees": 0.01, "slippage_bps": 7.0}}),                   # no reference price captured
        ("FILLED", {"environment": "LIVE", "instrument_key": key, "side": "LONG", "realized_costs": {"fees": 1.0}}),
    ]
    conn.executemany("INSERT INTO cati_execution_attempts VALUES (?, ?, ?)",
                     [(st, json.dumps(p), 1_760_000_000_000 + i) for i, (st, p) in enumerate(rows)])
    conn.commit()
    conn.close()
    recs = MC.demo_execution_records(db)
    assert [r["symbol"] for r in recs] == ["BTCUSDT", "ETHUSDT"]                              # one per fill; live rows not read
    assert recs[0]["exchange"] == "BINANCE_USDM" and recs[0]["order_type"] == "MARKET" and recs[0]["execution_latency"] is None
    first, second = recs
    assert first["observed_slippage_bps"] == pytest.approx(3.0) and first["fee_amount"] == 0.0005 and first["order_side"] == "BUY"
    assert first["bid_price"] is None and "spread_bps" in first["unavailable"]                # no book was captured: not invented
    assert second["observed_slippage_bps"] is None and second["expected_price"] is None       # no reference price: unavailable
    assert all(r["data_source"] == MC.DEMO_EXECUTION and r["measurement_quality"] == "DEMO_FILL_NOT_REPRESENTATIVE" for r in recs)
    assert "acct-secret" not in json.dumps(recs) and "u-1" not in json.dumps(recs)
    assert MC.demo_execution_records(tmp_path / "runtime.db") == recs
    empty = tmp_path / "empty.db"
    sqlite3.connect(empty).close()
    assert MC.demo_execution_records(empty) == []


def test_the_cost_table_reports_distributions_counts_and_keeps_extremes():
    recs = [book(bid=100.0, ask=100.0 + 0.01 * (i + 1), day=f"2026-10-{d:02d}", n=i) for d in range(1, 7) for i in range(6)]
    recs.append(book(bid=100.0, ask=105.0, day="2026-10-06", n=99))                           # a blown-out spread stays in
    recs += [fill(100.0, 100.0 + 0.01 * i, day=f"2026-10-{1 + i % 6:02d}", n=i) for i in range(40)]
    recs += [fill(100.0, 100.5, source=MC.DEMO_EXECUTION, n=200 + i) for i in range(50)]      # many demo fills
    recs += [MC.make_record(exchange="BINANCE", symbol="BTCUSDT", market_type="M", timestamp_utc="2026-10-01T00:00:00Z",
                            spread_bps=1.0, data_source=MC.ASSUMED, environment="R", measurement_quality="A", notes=str(i))
             for i in range(60)]
    t = MC.cost_table(recs)
    row = t["symbols"]["BINANCE:BTCUSDT"]
    assert row["spread_bps"]["n"] == 37 and row["spread_bps"]["distinct_days"] == 6 and row["spread_bps"]["status"] == MC.CALIBRATED
    assert row["spread_bps"]["max"] == pytest.approx(5.0 / 102.5 * 1e4)                       # the extreme is kept
    assert row["spread_bps"]["not_measured_samples"] == 60                                    # assumptions are counted apart
    assert row["slippage_bps"]["n"] == 40 and row["slippage_bps"]["demo_samples"] == 50       # demo fills do not calibrate
    assert row["slippage_bps"]["status"] == MC.CALIBRATED and row["funding_rate"]["status"] == MC.INSUFFICIENT_DATA
    assert row["by_provenance"] == {MC.LIVE_PUBLIC_OBSERVATION: 77, MC.DEMO_EXECUTION: 50, MC.ASSUMED: 60}
    assert MC.cost_table(recs)["table_hash"] == t["table_hash"]
    one_day = MC.cost_table([book(n=i) for i in range(100)])["symbols"]["BINANCE:BTCUSDT"]["spread_bps"]
    assert one_day["n"] == 100 and one_day["status"] == MC.INSUFFICIENT_DATA                   # one day is not a distribution
    demo_only = MC.cost_table([fill(source=MC.DEMO_EXECUTION, day=f"2026-10-{1 + i % 9:02d}", n=i) for i in range(90)])
    assert demo_only["symbols"]["BINANCE:BTCUSDT"]["slippage_bps"]["status"] == MC.INSUFFICIENT_DATA


def test_calibration_creates_a_new_version_and_never_touches_the_frozen_model():
    frozen = frozen_cost_model()
    before = json.dumps(frozen, sort_keys=True)
    thin = MC.cost_table([book(n=i) for i in range(10)])
    none = MC.calibrate(thin, frozen, version="20261009", symbols=["BTCUSDT"])
    assert none.status == MC.INSUFFICIENT_DATA and none.cost_model == frozen and "RETAINED" in none.basis
    assert "BTCUSDT:spread_bps:INSUFFICIENT_DATA" in none.reasons and "BTCUSDT:slippage_bps:INSUFFICIENT_DATA" in none.reasons
    assert MC.calibrate(thin, frozen, version="v", symbols=[]).status == MC.INSUFFICIENT_DATA
    recs = [book(bid=100.0, ask=100.04, day=f"2026-10-{d:02d}", n=i) for d in range(1, 7) for i in range(6)]
    recs += [fill(100.0, 100.02, day=f"2026-10-{1 + i % 6:02d}", n=i) for i in range(40)]
    cal = MC.calibrate(MC.cost_table(recs), frozen, version="20261015", symbols=["BTCUSDT"])
    assert cal.status == MC.CALIBRATED and cal.cost_model["cost_model_id"] != frozen["cost_model_id"]
    assert cal.cost_model["cost_model_version"] == "20261015" and cal.cost_model["cost_model_hash"] != frozen["cost_model_hash"]
    assert cal.cost_model["model"]["slippage"] == pytest.approx(2e-4) and cal.cost_model["model"]["spread"] == pytest.approx(0.04 / 100.02 / 2, rel=1e-6)
    assert json.dumps(frozen, sort_keys=True) == before and frozen_cost_model()["cost_model_hash"] == frozen["cost_model_hash"]
    partial = MC.calibrate(MC.cost_table(recs), frozen, version="v", symbols=["BTCUSDT", "ETHUSDT"])
    assert partial.status == MC.INSUFFICIENT_DATA                                              # every covered symbol, or none
