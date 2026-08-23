from __future__ import annotations

import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import admin_tradingview  # noqa: E402
from app.core.deps import require_admin  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402
from shared_lib.persistence.tradingview import (  # noqa: E402
    create_external_signal_queue_row,
    create_webhook,
    insert_advisory_decision,
    insert_alert,
    utc_now_iso,
)


def _make_client(tmp_path: Path) -> tuple[TestClient, DB]:
    db_path = tmp_path / "admin_tradingview.db"
    migrate(str(db_path))
    db = DB(path=str(db_path))
    app = FastAPI()
    app.include_router(admin_tradingview.router, prefix="/api")
    app.dependency_overrides[require_admin] = lambda: {"id": "admin-test", "role": "admin"}
    app.dependency_overrides[admin_tradingview._get_db] = lambda: db
    return TestClient(app), db


def test_admin_tradingview_lists_seeded_webhook_alert_and_decision(tmp_path):
    client, db = _make_client(tmp_path)
    seeded = create_webhook(db, bot_id="bot-admin-tv", allowed_symbols=["BTCUSDT"])
    now = utc_now_iso()
    alert_id = insert_alert(
        db,
        webhook_id=seeded["id"],
        bot_id="bot-admin-tv",
        alert_id="admin-alert-1",
        symbol_raw="BINANCE:BTCUSDT.P",
        symbol_normalized="BTCUSDT",
        action="BUY",
        side="LONG",
        timeframe="1m",
        strategy_name="Admin TV Test",
        price=100.0,
        payload={"alert_id": "admin-alert-1"},
        received_at=now,
        alert_timestamp=now,
        status="ACCEPTED_ADVISORY",
        reject_reason=None,
        idempotency_key="admin-idempotency-1",
        source_ip="127.0.0.1",
        signature_valid=None,
    )
    insert_advisory_decision(
        db,
        alert_row_id=alert_id,
        bot_id="bot-admin-tv",
        symbol="BTCUSDT",
        action="BUY",
        normalized_signal={"source": "TRADINGVIEW"},
    )
    queue_id = create_external_signal_queue_row(
        db,
        source_alert_id=str(alert_id),
        bot_id="bot-admin-tv",
        symbol="BTCUSDT",
        side="LONG",
        action="BUY",
        confidence=0.8,
        available_at=now,
        expires_at=now,
        result={"runner_processing_enabled": False},
    )

    webhooks = client.get("/api/admin/tradingview/webhooks").json()["items"]
    alerts = client.get("/api/admin/tradingview/alerts").json()["items"]
    decisions = client.get("/api/admin/tradingview/decisions").json()["items"]
    external_signals = client.get("/api/admin/tradingview/external-signals").json()["items"]

    assert webhooks[0]["id"] == seeded["id"]
    assert "token_hash" not in webhooks[0]
    assert alerts[0]["status"] == "ACCEPTED_ADVISORY"
    assert decisions[0]["final_status"] == "ACCEPTED_ADVISORY_ONLY"
    assert decisions[0]["execution_result"] is None
    assert external_signals[0]["id"] == queue_id
    assert external_signals[0]["status"] == "PENDING"
