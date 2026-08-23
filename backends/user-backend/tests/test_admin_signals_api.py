from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import admin_signals, signals  # noqa: E402
from app.api.auth import get_current_active_user  # noqa: E402
from app.core.deps import require_admin  # noqa: E402
from app.core.security import create_access_token  # noqa: E402
from shared_lib.persistence.db import DB, utc_now_iso  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402
from shared_lib.persistence.signals import (  # noqa: E402
    CANDIDATE_STATUS_REJECTED,
    SIGNAL_STATUS_ACTIVE,
    SIGNAL_STATUS_CANCELLED,
    SIGNAL_STATUS_EXPIRED,
    SIGNAL_STATUS_INVALIDATED,
    SIGNAL_STATUS_PENDING_ENTRY,
    create_signal_candidate,
    create_signal_performance,
    create_trading_signal,
    publish_trading_signal,
)


def _make_client(tmp_path: Path, *, admin: bool = True, user: bool = True) -> tuple[TestClient, DB]:
    db_path = tmp_path / "admin_signals_api.db"
    migrate(str(db_path))
    db = DB(path=str(db_path))
    app = FastAPI()
    app.include_router(admin_signals.router, prefix="/api")
    app.include_router(signals.router, prefix="/api/signals")
    app.dependency_overrides[admin_signals._get_db] = lambda: db
    app.dependency_overrides[signals._get_db] = lambda: db
    if admin:
        app.dependency_overrides[require_admin] = lambda: {"id": "admin-test", "role": "admin"}
    if user:
        app.dependency_overrides[get_current_active_user] = lambda: {"id": "user-test", "status": "active"}
    return TestClient(app), db


def _future_iso(hours: int = 4) -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()


def _seed_signal(
    db: DB,
    *,
    signal_id: str,
    status: str = SIGNAL_STATUS_PENDING_ENTRY,
    confidence_score: float = 82.0,
    risk_reward: float = 2.0,
    dev_mode: int = 0,
    expires_at: str | None = None,
) -> str:
    create_trading_signal(
        {
            "id": signal_id,
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "timeframe": "1h",
            "strategy_name": "Admin Signal Test",
            "entry_price": 100.0,
            "entry_zone_low": 99.5,
            "entry_zone_high": 100.5,
            "stop_loss": 95.0,
            "take_profit_1": 110.0,
            "take_profit_2": 115.0,
            "take_profit_3": 120.0,
            "risk_reward": risk_reward,
            "confidence_score": confidence_score,
            "signal_reason": "Test signal",
            "status": status,
            "expires_at": expires_at or _future_iso(),
            "dev_mode": dev_mode,
        },
        db=db,
    )
    return signal_id


def _seed_candidate(db: DB, *, candidate_id: str, dev_mode: int = 0) -> str:
    return create_signal_candidate(
        {
            "id": candidate_id,
            "asset_class": "crypto",
            "symbol": "ETHUSDT",
            "side": "SELL",
            "timeframe": "1h",
            "strategy_name": "Candidate Test",
            "entry_price": 200.0,
            "stop_loss": 210.0,
            "take_profit_1": 180.0,
            "risk_reward": 2.0,
            "confidence_score": 78.0,
            "signal_reason": "Rejected test setup",
            "rejection_reason": "LOW_CONFIDENCE",
            "status": CANDIDATE_STATUS_REJECTED,
            "dev_mode": dev_mode,
        },
        db=db,
    )


def test_admin_can_list_trading_signals_and_candidates(tmp_path):
    client, db = _make_client(tmp_path)
    _seed_signal(db, signal_id="sig-admin-list")
    _seed_candidate(db, candidate_id="cand-admin-list")

    signals_response = client.get("/api/admin/signals")
    candidates_response = client.get("/api/admin/signals/candidates")

    assert signals_response.status_code == 200
    assert signals_response.json()["items"][0]["id"] == "sig-admin-list"
    assert "candidate_id" in signals_response.json()["items"][0]
    assert candidates_response.status_code == 200
    assert candidates_response.json()["items"][0]["id"] == "cand-admin-list"
    assert candidates_response.json()["items"][0]["rejection_reason"] == "LOW_CONFIDENCE"


def test_admin_can_publish_valid_signal(tmp_path):
    client, db = _make_client(tmp_path)
    _seed_signal(db, signal_id="sig-publish")

    response = client.post("/api/admin/signals/sig-publish/publish")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == "sig-publish"
    assert payload["is_published"] == 1
    assert payload["published_at"] is not None


def test_publish_rejects_low_confidence_and_low_risk_reward(tmp_path):
    client, db = _make_client(tmp_path)
    _seed_signal(db, signal_id="sig-low-confidence", confidence_score=60.0)
    _seed_signal(db, signal_id="sig-low-rr", risk_reward=1.2)

    low_confidence = client.post("/api/admin/signals/sig-low-confidence/publish")
    low_rr = client.post("/api/admin/signals/sig-low-rr/publish")

    assert low_confidence.status_code == 400
    assert low_confidence.json()["detail"] == "LOW_CONFIDENCE"
    assert low_rr.status_code == 400
    assert low_rr.json()["detail"] == "LOW_RISK_REWARD"


def test_publish_rejects_missing_required_values_via_validator():
    base = {
        "id": "sig-validator",
        "confidence_score": 80,
        "risk_reward": 2,
        "entry_price": 100,
        "stop_loss": 95,
        "take_profit_1": 110,
        "expires_at": _future_iso(),
        "status": SIGNAL_STATUS_PENDING_ENTRY,
        "is_published": 0,
        "dev_mode": 0,
    }

    for field, reason in [
        ("entry_price", "MISSING_ENTRY_PRICE"),
        ("stop_loss", "MISSING_STOP_LOSS"),
        ("take_profit_1", "MISSING_TP1"),
        ("expires_at", "MISSING_EXPIRY"),
    ]:
        payload = dict(base)
        payload[field] = None
        try:
            admin_signals.validate_signal_publishable(payload)
        except Exception as exc:
            assert getattr(exc, "detail", None) == reason
        else:
            raise AssertionError(f"Expected validation failure for {field}")


def test_publish_rejects_expired_cancelled_invalidated_and_prod_dev_signal(tmp_path, monkeypatch):
    client, db = _make_client(tmp_path)
    _seed_signal(db, signal_id="sig-expired-time", expires_at=_future_iso(hours=-1))
    _seed_signal(db, signal_id="sig-cancelled", status=SIGNAL_STATUS_CANCELLED)
    _seed_signal(db, signal_id="sig-invalidated", status=SIGNAL_STATUS_INVALIDATED)
    _seed_signal(db, signal_id="sig-expired-status", status=SIGNAL_STATUS_EXPIRED)
    _seed_signal(db, signal_id="sig-dev", dev_mode=1)
    monkeypatch.setenv("APP_ENV", "production")

    assert client.post("/api/admin/signals/sig-expired-time/publish").json()["detail"] == "SIGNAL_EXPIRED"
    assert client.post("/api/admin/signals/sig-cancelled/publish").json()["detail"] == "SIGNAL_CANCELLED"
    assert client.post("/api/admin/signals/sig-invalidated/publish").json()["detail"] == "SIGNAL_INVALIDATED"
    assert client.post("/api/admin/signals/sig-expired-status/publish").json()["detail"] == "SIGNAL_EXPIRED"
    assert client.post("/api/admin/signals/sig-dev/publish").json()["detail"] == "DEV_SIGNAL_BLOCKED_IN_PRODUCTION"


def test_admin_can_list_unpublish_and_cancel_dev_signals(tmp_path):
    client, db = _make_client(tmp_path)
    _seed_signal(db, signal_id="sig-dev-list", dev_mode=1)
    _seed_candidate(db, candidate_id="cand-dev-list", dev_mode=1)
    publish_trading_signal("sig-dev-list", published_at=utc_now_iso(), db=db)

    signals_response = client.get("/api/admin/signals?dev_mode=1")
    candidates_response = client.get("/api/admin/signals/candidates?dev_mode=1")
    unpublish_response = client.post("/api/admin/signals/sig-dev-list/unpublish")
    cancel_response = client.post("/api/admin/signals/sig-dev-list/cancel")

    assert signals_response.status_code == 200
    assert signals_response.json()["items"][0]["id"] == "sig-dev-list"
    assert signals_response.json()["items"][0]["dev_mode"] == 1
    assert candidates_response.status_code == 200
    assert candidates_response.json()["items"][0]["id"] == "cand-dev-list"
    assert candidates_response.json()["items"][0]["dev_mode"] == 1
    assert unpublish_response.status_code == 200
    assert unpublish_response.json()["is_published"] == 0
    assert cancel_response.status_code == 200
    assert cancel_response.json()["status"] == SIGNAL_STATUS_CANCELLED


def test_admin_can_unpublish_and_user_api_no_longer_returns_signal(tmp_path):
    client, db = _make_client(tmp_path)
    _seed_signal(db, signal_id="sig-unpublish", status=SIGNAL_STATUS_ACTIVE)
    publish_trading_signal("sig-unpublish", published_at=utc_now_iso(), db=db)

    before = client.get("/api/signals/sig-unpublish")
    response = client.post("/api/admin/signals/sig-unpublish/unpublish")
    after = client.get("/api/signals/sig-unpublish")

    assert before.status_code == 200
    assert response.status_code == 200
    assert response.json()["is_published"] == 0
    assert after.status_code == 404


def test_admin_can_unpublish_auto_published_signal(tmp_path):
    client, db = _make_client(tmp_path)
    _seed_signal(db, signal_id="sig-auto-unpublish", status=SIGNAL_STATUS_PENDING_ENTRY)
    publish_trading_signal("sig-auto-unpublish", published_at=utc_now_iso(), db=db)

    active_before = client.get("/api/signals/active")
    response = client.post("/api/admin/signals/sig-auto-unpublish/unpublish")
    active_after = client.get("/api/signals/active")

    assert "sig-auto-unpublish" in {item["id"] for item in active_before.json()["items"]}
    assert response.status_code == 200
    assert response.json()["is_published"] == 0
    assert "sig-auto-unpublish" not in {item["id"] for item in active_after.json()["items"]}


def test_admin_can_cancel_and_user_active_api_no_longer_returns_signal(tmp_path):
    client, db = _make_client(tmp_path)
    _seed_signal(db, signal_id="sig-cancel", status=SIGNAL_STATUS_ACTIVE)
    publish_trading_signal("sig-cancel", published_at=utc_now_iso(), db=db)
    create_signal_performance(
        {
            "id": "perf-cancel",
            "signal_id": "sig-cancel",
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
        },
        db=db,
    )

    response = client.post("/api/admin/signals/sig-cancel/cancel")
    active = client.get("/api/signals/active")

    assert response.status_code == 200
    assert response.json()["status"] == SIGNAL_STATUS_CANCELLED
    assert response.json()["is_published"] == 0
    assert {item["id"] for item in active.json()["items"]} == set()
    with db.connect() as conn:
        perf = conn.execute("SELECT cancelled, result FROM signal_performance WHERE signal_id = ?", ("sig-cancel",)).fetchone()
    assert perf["cancelled"] == 1
    assert perf["result"] == "CANCELLED"


def test_normal_user_and_unauthenticated_user_cannot_access_admin_signals(tmp_path):
    unauth_client, _ = _make_client(tmp_path / "unauth", admin=False, user=False)
    user_client, db = _make_client(tmp_path / "user", admin=False, user=False)
    now = utc_now_iso()
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO users (id, email, hashed_password, status, role, is_verified, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("normal-user", "normal@example.com", "hash", "active", "user", 1, now, now),
        )
    token = create_access_token("normal-user", role="user")

    unauth = unauth_client.get("/api/admin/signals")
    normal_user = user_client.get("/api/admin/signals", headers={"Authorization": f"Bearer {token}"})

    assert unauth.status_code == 401
    assert normal_user.status_code == 401
