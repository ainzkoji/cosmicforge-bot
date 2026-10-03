import json
import time
from types import SimpleNamespace

import pytest
from shared_lib.persistence.db import DB
from shared_lib.persistence.market_data_schema import ensure_market_data_schema
from app.trading_intelligence.integration import forward_observe as observe

@pytest.fixture
def db(tmp_path):
    result = DB(path=str(tmp_path / "observe.db"))
    ensure_market_data_schema(result)
    return result

def fetch(path, params):
    if "depth" in path:
        return {"bids": [["100", "20"]], "asks": [["101", "20"]]}
    if "premiumIndex" in path:
        return {"markPrice": "100", "indexPrice": "99", "lastFundingRate": "0.0001", "nextFundingTime": 9999999999999}
    if "openInterest" in path:
        return {"openInterest": "400"}
    return [{"p": "100", "q": "2", "m": False, "T": int(time.time()*1000)-1000},
            {"p": "100", "q": "1", "m": True, "T": int(time.time()*1000)-500}]

def test_public_observations_persist_received_at_and_reference_provenance(db):
    assert observe.collect(db, fetch=fetch, symbols=("BTCUSDT",)) == 17
    with db.connect() as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM market_feature_observations")]
    assert len(rows) == 17
    available = [r for r in rows if r["status"] == "AVAILABLE"]
    assert len(available) == 15
    for row in available:
        meta = json.loads(row["value_json"])
        assert meta["availability"] == "FORWARD_OBSERVE_ONLY"
        assert meta["reference_market_only"] and meta["not_executed"]
        assert row["observed_at"] == meta["received_at"]
    flow = next(r for r in rows if r["feature"] == "aggressor_buy_quote_fraction")
    assert flow["value"] == pytest.approx(2/3)
    assert json.loads(flow["value_json"])["last_trade_at"] <= flow["observed_at"]
    cost = next(r for r in rows if r["feature"] == "execution_cost_proxy_bps")
    assert cost["value"] == pytest.approx(10000/100.5)
    assert all(r["value"] is None and r["unavailable_reason"] for r in rows if r["status"] == "UNAVAILABLE")

@pytest.mark.parametrize("bad", ["failure", "nonfinite", "future", "negative_trade"])
def test_failed_or_invalid_observation_is_unavailable_without_fabrication(db, bad):
    def broken(path, params):
        if bad == "failure":
            raise TimeoutError()
        if bad == "nonfinite" and "openInterest" in path:
            return {"openInterest": "nan"}
        if bad == "future" and "aggTrades" in path:
            return [{"p": "100", "q": "1", "m": False, "T": int(time.time()*1000)+60000}]
        if bad == "negative_trade" and "aggTrades" in path:
            return [{"p": "100", "q": "-1", "m": False, "T": int(time.time()*1000)-1000}]
        return fetch(path, params)
    observe.collect(db, fetch=broken, symbols=("BTCUSDT",))
    with db.connect() as c:
        feature = "open_interest" if bad not in ("future", "negative_trade") else "aggressor_buy_quote_fraction"
        row = c.execute("SELECT * FROM market_feature_observations WHERE feature=?", (feature,)).fetchone()
    assert row["status"] == "UNAVAILABLE" and row["value"] is None
    assert row["unavailable_reason"].startswith("PUBLIC_OBSERVATION_FAILED:")

def test_schedule_requires_current_canonical_process_lease(monkeypatch, db):
    monkeypatch.delenv("COSMICFORGE_TEST_MODE", raising=False)
    monkeypatch.setattr(observe, "_last", 0)
    monkeypatch.setattr(observe, "_future", None)
    monkeypatch.setattr("app.ops.runtime_ownership.current_owner", lambda *args: {"pid": -1, "heartbeat_at": "now"})
    from unittest.mock import Mock
    pool = Mock()
    monkeypatch.setattr(observe, "_pool", pool)
    observe.schedule(SimpleNamespace(db=db))
    pool.submit.assert_not_called()
