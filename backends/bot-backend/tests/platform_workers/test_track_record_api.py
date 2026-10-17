"""Steps 6.4 / 8.6: the public, read-only track-record API (both dialects)."""
from __future__ import annotations

import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import public_track_record as api
from app.golive import track_record as tr
from app.golive.track_record import DailyObservation, TrackRecordRecorder

ENV = {tr.PSEUDONYM_KEY_ENV: "q" * 40}


def _client(db):
    app = FastAPI()
    app.include_router(api.router)
    app.dependency_overrides[api.get_track_record_db] = lambda: db
    return TestClient(app)


def test_live_is_not_yet_available_without_a_store():
    r = _client(None).get("/api/v1/public/track-record")
    assert r.status_code == 200
    assert r.json()["live"] == {"state": "NOT_YET_AVAILABLE", "reason": "No authorized live trading has occurred"}
    assert all(k["series"] == [] for k in r.json()["kinds"].values())
    assert _client(None).get("/api/v1/public/track-record", params={"series_id": "demo-x"}).status_code == 404


def test_only_published_aggregates_are_served(platform_db):
    client = _client(platform_db)
    body = client.get("/api/v1/public/track-record").json()
    assert body["store"] == "AVAILABLE" and body["kinds"]["LIVE"]["state"] == "NOT_YET_AVAILABLE"
    rec = TrackRecordRecorder(platform_db, env=ENV)
    for a in "ABCDE":
        rec.record_day(DailyObservation("2026-03-02", "BINANCE_FUTURES_DEMO", "trend_core", "1.0.0", "BETA_INVITED",
                                        "BALANCED", "DEMO", "1000", "1010", "recon_run_9"),
                       account_id=f"user-{a}@example.com", publishable=True)
    # Recorded but not published: nothing is served.
    assert client.get("/api/v1/public/track-record").json()["kinds"]["DEMO"]["series"] == []
    pub = tr.publish_series(platform_db, {"source_kind": "DEMO", "family_id": "trend_core"})
    listing = client.get("/api/v1/public/track-record").json()
    assert [s["series_id"] for s in listing["kinds"]["DEMO"]["series"]] == [pub["series_id"]]
    detail = client.get("/api/v1/public/track-record", params={"series_id": pub["series_id"]})
    assert detail.status_code == 200 and detail.json()["label"] == "Simulated — not achieved trading results"
    text = json.dumps(listing) + detail.text
    for leak in ("example.com", "user-", "acct_", "recon_run", "account_ref", "ending_equity"):
        assert leak not in text
    assert client.get("/api/v1/public/track-record", params={"series_id": "missing"}).status_code == 404


def test_router_is_read_only():
    methods = {m for r in api.router.routes for m in r.methods}
    assert methods == {"GET"}
