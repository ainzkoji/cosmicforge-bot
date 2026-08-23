from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import signals  # noqa: E402
from app.api.auth import get_current_active_user  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402
from shared_lib.persistence.signals import (  # noqa: E402
    SIGNAL_STATUS_ACTIVE,
    SIGNAL_STATUS_CANCELLED,
    SIGNAL_STATUS_EXPIRED,
    SIGNAL_STATUS_INVALIDATED,
    SIGNAL_STATUS_SL_HIT,
    SIGNAL_STATUS_TP1_HIT,
    SIGNAL_STATUS_TP2_HIT,
    create_signal_notification,
    create_signal_performance,
    create_trading_signal,
    publish_trading_signal,
)


def _make_client(tmp_path: Path) -> tuple[TestClient, DB]:
    db_path = tmp_path / "signals_api.db"
    migrate(str(db_path))
    db = DB(path=str(db_path))
    app = FastAPI()
    app.include_router(signals.router, prefix="/api/signals")
    app.dependency_overrides[get_current_active_user] = lambda: {"id": "user-test", "status": "active"}
    app.dependency_overrides[signals._get_db] = lambda: db
    return TestClient(app), db


def _make_public_signal(
    db: DB,
    *,
    signal_id: str,
    status: str = SIGNAL_STATUS_ACTIVE,
    symbol: str = "BTCUSDT",
    side: str = "BUY",
    timeframe: str = "1h",
    confidence_score: float = 82.0,
    risk_reward: float = 2.0,
    dev_mode: int = 0,
    expires_delta: timedelta | None = None,
    published_delta: timedelta | None = None,
) -> str:
    now = datetime.now(timezone.utc)
    expires_at = now + (expires_delta if expires_delta is not None else timedelta(hours=4))
    published_at = now + (published_delta if published_delta is not None else timedelta(0))
    create_trading_signal(
        {
            "id": signal_id,
            "asset_class": "crypto",
            "symbol": symbol,
            "side": side,
            "timeframe": timeframe,
            "strategy_name": "Signal API Test",
            "entry_price": 100.0,
            "stop_loss": 95.0,
            "take_profit_1": 110.0,
            "take_profit_2": 115.0,
            "take_profit_3": 120.0,
            "risk_reward": risk_reward,
            "confidence_score": confidence_score,
            "signal_reason": "Test signal",
            "status": status,
            "expires_at": expires_at.isoformat(),
            "dev_mode": dev_mode,
        },
        db=db,
    )
    publish_trading_signal(signal_id, published_at=published_at.isoformat(), db=db)
    return signal_id


def _make_unpublished_signal(db: DB, *, signal_id: str) -> str:
    create_trading_signal(
        {
            "id": signal_id,
            "asset_class": "crypto",
            "symbol": "ETHUSDT",
            "side": "SELL",
            "entry_price": 200.0,
            "stop_loss": 210.0,
            "take_profit_1": 180.0,
            "risk_reward": 2.0,
            "confidence_score": 75.0,
            "status": SIGNAL_STATUS_ACTIVE,
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=3)).isoformat(),
        },
        db=db,
    )
    return signal_id


def test_authenticated_user_can_list_published_signals(tmp_path):
    client, db = _make_client(tmp_path)
    _make_public_signal(db, signal_id="sig-visible", symbol="BTCUSDT")
    _make_unpublished_signal(db, signal_id="sig-hidden")

    response = client.get("/api/signals")

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["items"][0]["id"] == "sig-visible"
    assert "candidate_id" not in payload["items"][0]
    assert payload["items"][0]["dev_mode"] == 0


def test_unauthenticated_request_is_blocked(tmp_path):
    db_path = tmp_path / "signals_api_unauth.db"
    migrate(str(db_path))
    db = DB(path=str(db_path))
    app = FastAPI()
    app.include_router(signals.router, prefix="/api/signals")
    app.dependency_overrides[signals._get_db] = lambda: db

    response = TestClient(app).get("/api/signals")

    assert response.status_code == 401


def test_active_endpoint_excludes_expired_cancelled_invalidated_and_sl_hit(tmp_path):
    client, db = _make_client(tmp_path)
    _make_public_signal(db, signal_id="sig-active", status=SIGNAL_STATUS_ACTIVE)
    _make_public_signal(
        db,
        signal_id="sig-expired-by-time",
        status=SIGNAL_STATUS_ACTIVE,
        expires_delta=timedelta(hours=-1),
    )
    _make_public_signal(db, signal_id="sig-cancelled", status=SIGNAL_STATUS_CANCELLED)
    _make_public_signal(db, signal_id="sig-invalidated", status=SIGNAL_STATUS_INVALIDATED)
    _make_public_signal(db, signal_id="sig-sl-hit", status=SIGNAL_STATUS_SL_HIT)

    response = client.get("/api/signals/active")

    assert response.status_code == 200
    ids = {item["id"] for item in response.json()["items"]}
    assert ids == {"sig-active"}


def test_history_endpoint_returns_completed_published_signals(tmp_path):
    client, db = _make_client(tmp_path)
    _make_public_signal(db, signal_id="sig-history", status=SIGNAL_STATUS_EXPIRED)
    _make_public_signal(db, signal_id="sig-active", status=SIGNAL_STATUS_ACTIVE)

    response = client.get("/api/signals/history")

    assert response.status_code == 200
    ids = {item["id"] for item in response.json()["items"]}
    assert ids == {"sig-history"}


def test_history_endpoint_returns_expired_by_time_pending_signals_as_expired(tmp_path):
    client, db = _make_client(tmp_path)
    _make_public_signal(
        db,
        signal_id="sig-expired-pending",
        status=SIGNAL_STATUS_ACTIVE,
        expires_delta=timedelta(hours=-1),
    )

    response = client.get("/api/signals/history?status=EXPIRED")

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["items"][0]["id"] == "sig-expired-pending"
    assert payload["items"][0]["status"] == SIGNAL_STATUS_EXPIRED


def test_detail_returns_published_signal_and_hides_unpublished(tmp_path):
    client, db = _make_client(tmp_path)
    _make_public_signal(db, signal_id="sig-detail")
    _make_unpublished_signal(db, signal_id="sig-unpublished")

    published = client.get("/api/signals/sig-detail")
    hidden = client.get("/api/signals/sig-unpublished")

    assert published.status_code == 200
    assert published.json()["id"] == "sig-detail"
    assert published.json()["disclaimer"] == signals.DISCLAIMER
    assert isinstance(published.json()["time_left_seconds"], int)
    assert hidden.status_code == 404


def test_production_hides_dev_mode_signals(tmp_path, monkeypatch):
    client, db = _make_client(tmp_path)
    _make_public_signal(db, signal_id="sig-real", dev_mode=0)
    _make_public_signal(db, signal_id="sig-dev", dev_mode=1)
    monkeypatch.setenv("APP_ENV", "production")

    response = client.get("/api/signals")

    assert response.status_code == 200
    ids = {item["id"] for item in response.json()["items"]}
    assert ids == {"sig-real"}

    active = client.get("/api/signals/active")
    history = client.get("/api/signals/history")
    detail = client.get("/api/signals/sig-dev")

    assert {item["id"] for item in active.json()["items"]} == {"sig-real"}
    assert {item["id"] for item in history.json()["items"]} == set()
    assert detail.status_code == 404


def test_production_performance_excludes_dev_mode_signals(tmp_path, monkeypatch):
    client, db = _make_client(tmp_path)
    _make_public_signal(db, signal_id="sig-real", status=SIGNAL_STATUS_TP2_HIT, dev_mode=0)
    _make_public_signal(db, signal_id="sig-dev", status=SIGNAL_STATUS_TP2_HIT, dev_mode=1)
    monkeypatch.setenv("APP_ENV", "production")

    response = client.get("/api/signals/performance")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_signals"] == 1
    assert payload["completed_signals"] == 1
    assert payload["win_rate"] == 100.0


def test_performance_returns_safe_empty_response_without_completed_data(tmp_path):
    client, db = _make_client(tmp_path)
    _make_public_signal(db, signal_id="sig-active", status=SIGNAL_STATUS_ACTIVE)

    response = client.get("/api/signals/performance")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_signals"] == 1
    assert payload["completed_signals"] == 0
    assert payload["tp1_hit_rate"] is None
    assert payload["message"] == "Performance data will appear after enough completed signals."


def test_performance_win_rate_does_not_count_tp1_only_as_win(tmp_path):
    client, db = _make_client(tmp_path)
    _make_public_signal(db, signal_id="sig-tp1-only", status=SIGNAL_STATUS_TP1_HIT)
    _make_public_signal(db, signal_id="sig-tp2-win", status=SIGNAL_STATUS_TP2_HIT)
    create_signal_performance(
        {
            "id": "perf-tp1-only",
            "signal_id": "sig-tp1-only",
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "entry_triggered": 1,
            "tp1_hit": 1,
            "expired": 1,
            "result": "EXPIRED",
        },
        db=db,
    )
    create_signal_performance(
        {
            "id": "perf-tp2-win",
            "signal_id": "sig-tp2-win",
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "entry_triggered": 1,
            "tp1_hit": 1,
            "tp2_hit": 1,
            "result": "WIN",
        },
        db=db,
    )

    response = client.get("/api/signals/performance")

    assert response.status_code == 200
    payload = response.json()
    assert payload["completed_signals"] == 2
    assert payload["tp1_hit_rate"] == 100.0
    assert payload["tp2_hit_rate"] == 50.0
    assert payload["win_rate"] == 50.0


def test_get_preferences_creates_defaults_and_update_validates(tmp_path):
    client, _ = _make_client(tmp_path)

    defaults = client.get("/api/signals/preferences")
    invalid_confidence = client.put("/api/signals/preferences", json={"minimum_confidence": 40})
    invalid_risk = client.put("/api/signals/preferences", json={"risk_style": "reckless"})
    updated = client.put(
        "/api/signals/preferences",
        json={
            "minimum_confidence": 80,
            "risk_style": "conservative",
            "majors_only": True,
            "notifications_enabled": False,
            "notify_tp1_hit": True,
        },
    )

    assert defaults.status_code == 200
    assert defaults.json()["favorite_symbols"] == []
    assert defaults.json()["hidden_symbols"] == []
    assert defaults.json()["risk_style"] == "balanced"
    assert invalid_confidence.status_code == 400
    assert invalid_confidence.json()["detail"] == "MINIMUM_CONFIDENCE_OUT_OF_RANGE"
    assert invalid_risk.status_code == 400
    assert invalid_risk.json()["detail"] == "INVALID_RISK_STYLE"
    assert updated.status_code == 200
    assert updated.json()["minimum_confidence"] == 80.0
    assert updated.json()["risk_style"] == "conservative"
    assert updated.json()["majors_only"] is True
    assert updated.json()["notifications_enabled"] is False
    assert updated.json()["notify_tp1_hit"] is True


def test_favorite_and_hidden_symbol_preferences_filter_signals(tmp_path):
    client, db = _make_client(tmp_path)
    _make_public_signal(db, signal_id="sig-btc", symbol="BTCUSDT")
    _make_public_signal(db, signal_id="sig-eth", symbol="ETHUSDT")
    _make_public_signal(db, signal_id="sig-doge", symbol="DOGEUSDT")

    add_fav = client.post("/api/signals/preferences/favorites/BTCUSDT")
    add_hidden = client.post("/api/signals/preferences/hidden/DOGEUSDT")
    favorites = client.get("/api/signals?favorites_only=1")
    visible = client.get("/api/signals")
    include_hidden = client.get("/api/signals?include_hidden=1")
    remove_fav = client.delete("/api/signals/preferences/favorites/BTCUSDT")
    remove_hidden = client.delete("/api/signals/preferences/hidden/DOGEUSDT")

    assert add_fav.status_code == 200
    assert add_fav.json()["favorite_symbols"] == ["BTCUSDT"]
    assert add_hidden.status_code == 200
    assert add_hidden.json()["hidden_symbols"] == ["DOGEUSDT"]
    assert {item["id"] for item in favorites.json()["items"]} == {"sig-btc"}
    assert {item["id"] for item in visible.json()["items"]} == {"sig-btc", "sig-eth"}
    assert {item["id"] for item in include_hidden.json()["items"]} == {"sig-btc", "sig-eth", "sig-doge"}
    assert remove_fav.json()["favorite_symbols"] == []
    assert remove_hidden.json()["hidden_symbols"] == []


def test_signal_filters_and_sorting(tmp_path):
    client, db = _make_client(tmp_path)
    _make_public_signal(
        db,
        signal_id="sig-btc-new",
        symbol="BTCUSDT",
        side="BUY",
        timeframe="15m",
        confidence_score=90,
        risk_reward=2.1,
        published_delta=timedelta(minutes=2),
    )
    _make_public_signal(
        db,
        signal_id="sig-eth-old",
        symbol="ETHUSDT",
        side="SELL",
        timeframe="1h",
        confidence_score=72,
        risk_reward=3.5,
        published_delta=timedelta(minutes=-10),
    )
    _make_public_signal(
        db,
        signal_id="sig-link",
        symbol="LINKUSDT",
        side="BUY",
        timeframe="30m",
        confidence_score=84,
        risk_reward=1.9,
    )

    search = client.get("/api/signals?search=eth")
    side = client.get("/api/signals?side=BUY")
    timeframe = client.get("/api/signals?timeframe=15m")
    confidence = client.get("/api/signals?min_confidence=85")
    majors = client.get("/api/signals?majors_only=1")
    sort_rr = client.get("/api/signals?sort=risk_reward")

    assert {item["id"] for item in search.json()["items"]} == {"sig-eth-old"}
    assert {item["id"] for item in side.json()["items"]} == {"sig-btc-new", "sig-link"}
    assert {item["id"] for item in timeframe.json()["items"]} == {"sig-btc-new"}
    assert {item["id"] for item in confidence.json()["items"]} == {"sig-btc-new"}
    assert {item["id"] for item in majors.json()["items"]} == {"sig-btc-new", "sig-eth-old"}
    assert sort_rr.json()["items"][0]["id"] == "sig-eth-old"


def test_status_filter_and_active_history_filters(tmp_path):
    client, db = _make_client(tmp_path)
    _make_public_signal(db, signal_id="sig-active-buy", symbol="BTCUSDT", status=SIGNAL_STATUS_ACTIVE, side="BUY")
    _make_public_signal(db, signal_id="sig-active-sell", symbol="ETHUSDT", status=SIGNAL_STATUS_ACTIVE, side="SELL")
    _make_public_signal(db, signal_id="sig-expired", symbol="BTCUSDT", status=SIGNAL_STATUS_EXPIRED)

    active_sell = client.get("/api/signals/active?side=SELL")
    expired = client.get("/api/signals/history?status=EXPIRED")

    assert {item["id"] for item in active_sell.json()["items"]} == {"sig-active-sell"}
    assert {item["id"] for item in expired.json()["items"]} == {"sig-expired"}


def test_signal_notifications_can_be_listed_and_marked_read(tmp_path):
    client, db = _make_client(tmp_path)
    create_signal_notification(
        {
            "id": "notif-broadcast",
            "signal_id": "sig-1",
            "symbol": "BTCUSDT",
            "event_type": "NEW_SIGNAL_PUBLISHED",
            "title": "New signal",
            "message": "BTCUSDT signal is available.",
        },
        db=db,
    )
    create_signal_notification(
        {
            "id": "notif-other-user",
            "user_id": "someone-else",
            "event_type": "TP2_HIT",
            "title": "Other",
            "message": "Should not be visible.",
        },
        db=db,
    )

    listed = client.get("/api/signals/notifications")
    read = client.post("/api/signals/notifications/notif-broadcast/read")
    relisted = client.get("/api/signals/notifications?status=READ")

    assert listed.status_code == 200
    assert {item["id"] for item in listed.json()["items"]} == {"notif-broadcast"}
    assert read.status_code == 200
    assert relisted.json()["items"][0]["id"] == "notif-broadcast"


def test_unauthenticated_preference_access_is_blocked(tmp_path):
    db_path = tmp_path / "signals_pref_unauth.db"
    migrate(str(db_path))
    db = DB(path=str(db_path))
    app = FastAPI()
    app.include_router(signals.router, prefix="/api/signals")
    app.dependency_overrides[signals._get_db] = lambda: db

    response = TestClient(app).get("/api/signals/preferences")

    assert response.status_code == 401
