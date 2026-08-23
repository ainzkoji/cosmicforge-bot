from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import tradingview
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate
from shared_lib.persistence.tradingview import (
    MODE_EXTERNAL_SIGNAL_CANDIDATE,
    create_webhook,
    list_alerts,
    list_decisions,
    list_external_signal_queue,
    utc_now_iso,
)
from shared_lib.tradingview_symbol_mapper import normalize_tradingview_symbol


def _make_client(tmp_path, monkeypatch) -> tuple[TestClient, DB, dict]:
    db_path = tmp_path / "tradingview.db"
    migrate(str(db_path))
    db = DB(path=str(db_path))
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT,ETHUSDT")
    monkeypatch.setenv("LIVE_SYMBOLS", "BTCUSDT,ETHUSDT")
    monkeypatch.setattr(tradingview, "_get_db", lambda: db)
    app = FastAPI()
    app.include_router(tradingview.router, prefix="/api/v1/tradingview")
    seeded = create_webhook(db, bot_id="bot-tv-test", allowed_symbols=["BTCUSDT", "ETHUSDT"])
    return TestClient(app), db, seeded


def _queue_count(db: DB) -> int:
    with db.connect() as conn:
        return conn.execute("SELECT COUNT(*) AS c FROM external_signal_queue").fetchone()["c"]


def _payload(token: str, **overrides):
    data = {
        "token": token,
        "bot_id": "bot-tv-test",
        "alert_id": "alert-1",
        "symbol": "BINANCE:BTCUSDT.P",
        "exchange": "BINANCE",
        "timeframe": "1m",
        "strategy_name": "TV Test",
        "action": "BUY",
        "side": "LONG",
        "price": "100.5",
        "timestamp": utc_now_iso(),
        "confidence": 0.75,
    }
    data.update(overrides)
    return data


def test_valid_alert_stored_as_advisory_only_without_execution(tmp_path, monkeypatch):
    client, db, seeded = _make_client(tmp_path, monkeypatch)

    res = client.post(f"/api/v1/tradingview/webhook/{seeded['token']}", json=_payload(seeded["token"]))

    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "accepted"
    assert body["mode"] == "ADVISORY_ONLY"
    assert body["execution_enabled"] is False
    alerts = list_alerts(db)
    decisions = list_decisions(db)
    assert alerts[0]["status"] == "ACCEPTED_ADVISORY"
    assert decisions[0]["final_status"] == "ACCEPTED_ADVISORY_ONLY"
    assert decisions[0]["final_reason"] == "Alert accepted; advisory-only mode; no queue row created."
    assert decisions[0]["execution_result"] is None
    assert decisions[0]["decision_trace_id"] is None
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) AS c FROM external_signal_queue").fetchone()["c"] == 0
        trade_count = conn.execute("SELECT COUNT(*) AS c FROM trade_fills").fetchone()["c"]
    assert trade_count == 0


def test_missing_or_invalid_token_rejected(tmp_path, monkeypatch):
    client, db, seeded = _make_client(tmp_path, monkeypatch)
    bad = _payload(seeded["token"])
    bad.pop("token")

    res = client.post("/api/v1/tradingview/webhook/not-a-token", json=bad)

    assert res.json()["status"] == "rejected"
    assert res.json()["reason"] == "INVALID_TOKEN"


def test_disabled_webhook_rejected(tmp_path, monkeypatch):
    client, db, seeded = _make_client(tmp_path, monkeypatch)
    with db.connect() as conn:
        conn.execute("UPDATE tradingview_webhooks SET is_enabled=0 WHERE id=?", (seeded["id"],))

    res = client.post(f"/api/v1/tradingview/webhook/{seeded['token']}", json=_payload(seeded["token"]))

    assert res.json()["reason"] == "DISABLED_WEBHOOK"
    assert _queue_count(db) == 0


def test_stale_timestamp_rejected(tmp_path, monkeypatch):
    client, db, seeded = _make_client(tmp_path, monkeypatch)
    stale = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()

    res = client.post(
        f"/api/v1/tradingview/webhook/{seeded['token']}",
        json=_payload(seeded["token"], timestamp=stale),
    )

    assert res.json()["reason"] == "STALE_ALERT"
    assert _queue_count(db) == 0


def test_duplicate_alert_rejected_without_second_decision(tmp_path, monkeypatch):
    client, db, seeded = _make_client(tmp_path, monkeypatch)
    payload = _payload(seeded["token"])

    assert client.post(f"/api/v1/tradingview/webhook/{seeded['token']}", json=payload).json()["status"] == "accepted"
    duplicate = client.post(f"/api/v1/tradingview/webhook/{seeded['token']}", json=payload)

    assert duplicate.json()["status"] == "duplicate"
    assert duplicate.json()["reason"] == "DUPLICATE_ALERT"
    assert len(list_decisions(db)) == 1
    assert _queue_count(db) == 0


def test_invalid_signature_rejected_when_header_present(tmp_path, monkeypatch):
    client, db, seeded = _make_client(tmp_path, monkeypatch)

    res = client.post(
        f"/api/v1/tradingview/webhook/{seeded['token']}",
        json=_payload(seeded["token"]),
        headers={
            "X-CF-TV-Signature": "sha256=bad",
            "X-CF-TV-Timestamp": "1",
            "X-CF-TV-Nonce": "n",
        },
    )

    assert res.json()["reason"] == "INVALID_SIGNATURE"
    assert _queue_count(db) == 0


def test_valid_signature_accepted(tmp_path, monkeypatch):
    client, db, seeded = _make_client(tmp_path, monkeypatch)
    body = json.dumps(_payload(seeded["token"]), separators=(",", ":")).encode("utf-8")
    ts = "123"
    nonce = "nonce-1"
    signed = ts.encode() + b"." + nonce.encode() + b"." + body
    sig = hmac.new(seeded["token"].encode(), signed, hashlib.sha256).hexdigest()

    res = client.post(
        f"/api/v1/tradingview/webhook/{seeded['token']}",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-CF-TV-Signature": f"sha256={sig}",
            "X-CF-TV-Timestamp": ts,
            "X-CF-TV-Nonce": nonce,
        },
    )

    assert res.json()["status"] == "accepted"


def test_invalid_schema_and_side_conflict_rejected(tmp_path, monkeypatch):
    client, db, seeded = _make_client(tmp_path, monkeypatch)
    invalid = _payload(seeded["token"])
    invalid.pop("alert_id")

    assert client.post(f"/api/v1/tradingview/webhook/{seeded['token']}", json=invalid).json()["reason"] == "INVALID_SCHEMA"
    conflict = _payload(seeded["token"], alert_id="alert-side", action="BUY", side="SHORT")
    assert client.post(f"/api/v1/tradingview/webhook/{seeded['token']}", json=conflict).json()["reason"] == "SIDE_ACTION_MISMATCH"


def test_unsupported_actions_rejected(tmp_path, monkeypatch):
    client, db, seeded = _make_client(tmp_path, monkeypatch)
    for action in ["CLOSE", "REVERSE"]:
        res = client.post(
            f"/api/v1/tradingview/webhook/{seeded['token']}",
            json=_payload(seeded["token"], alert_id=f"alert-{action}", action=action),
        )
        assert res.json()["reason"] == "UNSUPPORTED_ACTION"
    assert _queue_count(db) == 0


def test_unsupported_symbol_and_exchange_mismatch_rejected(tmp_path, monkeypatch):
    client, db, seeded = _make_client(tmp_path, monkeypatch)
    unsupported = client.post(
        f"/api/v1/tradingview/webhook/{seeded['token']}",
        json=_payload(seeded["token"], alert_id="bad-symbol", symbol="DOGEUSDT"),
    )
    mismatch = client.post(
        f"/api/v1/tradingview/webhook/{seeded['token']}",
        json=_payload(seeded["token"], alert_id="bad-exchange", symbol="BYBIT:BTCUSDT.P"),
    )

    assert unsupported.json()["reason"] == "SYMBOL_NOT_ALLOWED_BY_WEBHOOK"
    assert mismatch.json()["reason"] == "EXCHANGE_MISMATCH"
    assert _queue_count(db) == 0


def test_buy_sell_side_mapping_and_symbol_normalization(tmp_path, monkeypatch):
    assert normalize_tradingview_symbol("BINANCE:BTCUSDT.P") == "BTCUSDT"
    assert normalize_tradingview_symbol("BTC/USDT") == "BTCUSDT"
    assert normalize_tradingview_symbol("BTCUSDT.P") == "BTCUSDT"
    client, db, seeded = _make_client(tmp_path, monkeypatch)

    buy = client.post(
        f"/api/v1/tradingview/webhook/{seeded['token']}",
        json=_payload(seeded["token"], alert_id="buy-map", action="BUY", side=None),
    )
    sell = client.post(
        f"/api/v1/tradingview/webhook/{seeded['token']}",
        json=_payload(seeded["token"], alert_id="sell-map", action="SELL", side=None, symbol="BTC/USDT"),
    )

    assert buy.json()["status"] == "accepted"
    assert sell.json()["status"] == "accepted"
    alerts = list_alerts(db)
    sides = {row["alert_id"]: row["side"] for row in alerts}
    assert sides["buy-map"] == "LONG"
    assert sides["sell-map"] == "SHORT"


def test_external_signal_candidate_buy_enqueues_pending_row_without_execution(tmp_path, monkeypatch):
    client, db, seeded = _make_client(tmp_path, monkeypatch)
    with db.connect() as conn:
        conn.execute(
            "UPDATE tradingview_webhooks SET mode=? WHERE id=?",
            (MODE_EXTERNAL_SIGNAL_CANDIDATE, seeded["id"]),
        )

    res = client.post(
        f"/api/v1/tradingview/webhook/{seeded['token']}",
        json=_payload(seeded["token"], alert_id="candidate-buy", action="BUY", side="LONG"),
    )

    body = res.json()
    assert body["status"] == "accepted"
    assert body["mode"] == MODE_EXTERNAL_SIGNAL_CANDIDATE
    assert body["execution_enabled"] is False
    assert body["queue_id"]
    queue = list_external_signal_queue(db)
    decisions = list_decisions(db)
    assert len(queue) == 1
    assert queue[0]["status"] == "PENDING"
    assert queue[0]["source"] == "TRADINGVIEW"
    assert queue[0]["symbol"] == "BTCUSDT"
    assert queue[0]["action"] == "BUY"
    assert queue[0]["side"] == "LONG"
    assert queue[0]["claimed_at"] is None
    assert queue[0]["processed_at"] is None
    assert decisions[0]["final_status"] == "QUEUED_EXTERNAL_SIGNAL"
    assert decisions[0]["execution_result"] == "NOT_APPLICABLE"
    assert decisions[0]["queue_id"] == queue[0]["id"]
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) AS c FROM trade_fills").fetchone()["c"] == 0


def test_external_signal_candidate_sell_enqueues_pending_row(tmp_path, monkeypatch):
    client, db, seeded = _make_client(tmp_path, monkeypatch)
    with db.connect() as conn:
        conn.execute(
            "UPDATE tradingview_webhooks SET mode=? WHERE id=?",
            (MODE_EXTERNAL_SIGNAL_CANDIDATE, seeded["id"]),
        )

    res = client.post(
        f"/api/v1/tradingview/webhook/{seeded['token']}",
        json=_payload(seeded["token"], alert_id="candidate-sell", action="SELL", side="SHORT", symbol="BTC/USDT"),
    )

    assert res.json()["queue_id"]
    queue = list_external_signal_queue(db)
    assert len(queue) == 1
    assert queue[0]["status"] == "PENDING"
    assert queue[0]["action"] == "SELL"
    assert queue[0]["side"] == "SHORT"


def test_candidate_duplicate_does_not_create_duplicate_queue_rows(tmp_path, monkeypatch):
    client, db, seeded = _make_client(tmp_path, monkeypatch)
    with db.connect() as conn:
        conn.execute(
            "UPDATE tradingview_webhooks SET mode=? WHERE id=?",
            (MODE_EXTERNAL_SIGNAL_CANDIDATE, seeded["id"]),
        )
    payload = _payload(seeded["token"], alert_id="candidate-duplicate")

    first = client.post(f"/api/v1/tradingview/webhook/{seeded['token']}", json=payload)
    second = client.post(f"/api/v1/tradingview/webhook/{seeded['token']}", json=payload)

    assert first.json()["queue_id"]
    assert second.json()["status"] == "duplicate"
    assert len(list_external_signal_queue(db)) == 1
