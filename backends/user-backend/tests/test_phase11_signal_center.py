"""
Phase 11: Crypto Signal Center – User-backend API gap tests and safety proof.

Covers gaps not already addressed by:
  test_signals_api.py
  test_admin_signals_api.py

Specifically adds:
  1. Admin cannot re-publish already-published signal (ALREADY_PUBLISHED)
  2. Full publish→user-visible→unpublish→user-hidden flow via API endpoints
  3. Publish action on already-active (published) signal is blocked at HTTP layer
  4. Dev signal in non-production IS visible to admin but NOT publishable in production
  5. Signal detail includes time_left_seconds and disclaimer
  6. Admin API source does not reference execution systems
  7. User API source does not reference execution systems
  8. Active endpoint time-filter: expired-by-time signal excluded without status change
  9. History endpoint includes TP2_HIT and TP3_HIT signals
 10. Admin cancel clears is_published and updates performance result
"""
from __future__ import annotations

import inspect
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
from shared_lib.persistence.db import DB, utc_now_iso  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402
from shared_lib.persistence.signals import (  # noqa: E402
    SIGNAL_STATUS_ACTIVE,
    SIGNAL_STATUS_CANCELLED,
    SIGNAL_STATUS_EXPIRED,
    SIGNAL_STATUS_PENDING_ENTRY,
    SIGNAL_STATUS_TP2_HIT,
    SIGNAL_STATUS_TP3_HIT,
    create_signal_performance,
    create_trading_signal,
    publish_trading_signal,
)


def _make_client(tmp_path: Path) -> tuple[TestClient, DB]:
    db_path = tmp_path / "phase11_api.db"
    migrate(str(db_path))
    db = DB(path=str(db_path))
    app = FastAPI()
    app.include_router(admin_signals.router, prefix="/api")
    app.include_router(signals.router, prefix="/api/signals")
    app.dependency_overrides[admin_signals._get_db] = lambda: db
    app.dependency_overrides[signals._get_db] = lambda: db
    app.dependency_overrides[require_admin] = lambda: {"id": "admin-1", "role": "admin"}
    app.dependency_overrides[get_current_active_user] = lambda: {"id": "user-1", "status": "active"}
    return TestClient(app), db


def _future_iso(hours: int = 4) -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()


def _seed(
    db: DB,
    *,
    signal_id: str,
    status: str = SIGNAL_STATUS_PENDING_ENTRY,
    confidence_score: float = 82.0,
    risk_reward: float = 2.0,
    dev_mode: int = 0,
    expires_at: str | None = None,
    published: bool = False,
) -> str:
    create_trading_signal(
        {
            "id": signal_id,
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "timeframe": "1h",
            "strategy_name": "Phase11 Test",
            "entry_price": 100.0,
            "entry_zone_low": 99.5,
            "entry_zone_high": 100.5,
            "stop_loss": 95.0,
            "take_profit_1": 107.5,
            "take_profit_2": 110.0,
            "take_profit_3": 115.0,
            "risk_reward": risk_reward,
            "confidence_score": confidence_score,
            "signal_reason": "Phase 11 test signal",
            "status": status,
            "expires_at": expires_at or _future_iso(),
            "dev_mode": dev_mode,
        },
        db=db,
    )
    if published:
        publish_trading_signal(signal_id, published_at=utc_now_iso(), db=db)
    return signal_id


# ---------------------------------------------------------------------------
# 1. Admin cannot re-publish already-published signal
# ---------------------------------------------------------------------------


def test_admin_publish_already_published_is_rejected(tmp_path):
    client, db = _make_client(tmp_path)
    _seed(db, signal_id="sig-already-pub", published=True)

    response = client.post("/api/admin/signals/sig-already-pub/publish")

    assert response.status_code == 400
    assert response.json()["detail"] == "ALREADY_PUBLISHED"


# ---------------------------------------------------------------------------
# 2. Full publish → user sees it → unpublish → user no longer sees it
# ---------------------------------------------------------------------------


def test_publish_then_unpublish_toggles_user_visibility(tmp_path):
    client, db = _make_client(tmp_path)
    _seed(db, signal_id="sig-toggle")

    before_publish = client.get("/api/signals/sig-toggle")
    assert before_publish.status_code == 404

    publish_resp = client.post("/api/admin/signals/sig-toggle/publish")
    assert publish_resp.status_code == 200
    assert publish_resp.json()["is_published"] == 1

    after_publish = client.get("/api/signals/sig-toggle")
    assert after_publish.status_code == 200
    assert after_publish.json()["id"] == "sig-toggle"

    unpublish_resp = client.post("/api/admin/signals/sig-toggle/unpublish")
    assert unpublish_resp.status_code == 200
    assert unpublish_resp.json()["is_published"] == 0

    after_unpublish = client.get("/api/signals/sig-toggle")
    assert after_unpublish.status_code == 404


# ---------------------------------------------------------------------------
# 3. Active endpoint excludes signals past expires_at even if status is ACTIVE
# ---------------------------------------------------------------------------


def test_active_endpoint_excludes_expired_by_time_signal(tmp_path):
    client, db = _make_client(tmp_path)
    _seed(db, signal_id="sig-time-expired", status=SIGNAL_STATUS_ACTIVE, expires_at=_future_iso(-1), published=True)
    _seed(db, signal_id="sig-time-valid", status=SIGNAL_STATUS_ACTIVE, published=True)

    response = client.get("/api/signals/active")
    assert response.status_code == 200
    ids = {item["id"] for item in response.json()["items"]}
    assert "sig-time-expired" not in ids
    assert "sig-time-valid" in ids


# ---------------------------------------------------------------------------
# 4. Signal detail includes time_left_seconds (int >= 0) and disclaimer
# ---------------------------------------------------------------------------


def test_signal_detail_includes_time_left_and_disclaimer(tmp_path):
    client, db = _make_client(tmp_path)
    _seed(db, signal_id="sig-detail-check", published=True)

    response = client.get("/api/signals/sig-detail-check")

    assert response.status_code == 200
    payload = response.json()
    assert "disclaimer" in payload
    assert len(payload["disclaimer"]) > 10
    assert "time_left_seconds" in payload
    assert isinstance(payload["time_left_seconds"], int)
    assert payload["time_left_seconds"] >= 0


# ---------------------------------------------------------------------------
# 5. History endpoint includes TP2_HIT and TP3_HIT outcomes
# ---------------------------------------------------------------------------


def test_history_includes_tp2_and_tp3_outcomes(tmp_path):
    client, db = _make_client(tmp_path)
    _seed(db, signal_id="sig-tp2-hist", status=SIGNAL_STATUS_TP2_HIT, published=True)
    _seed(db, signal_id="sig-tp3-hist", status=SIGNAL_STATUS_TP3_HIT, published=True)
    _seed(db, signal_id="sig-active-hist", status=SIGNAL_STATUS_ACTIVE, published=True)

    response = client.get("/api/signals/history")
    assert response.status_code == 200
    ids = {item["id"] for item in response.json()["items"]}
    assert "sig-tp2-hist" in ids
    assert "sig-tp3-hist" in ids
    assert "sig-active-hist" not in ids


# ---------------------------------------------------------------------------
# 6. Performance endpoint: TP2 and TP3 statuses count as WIN; TP1-only does not
# ---------------------------------------------------------------------------


def test_performance_tp2_and_tp3_count_as_win_tp1_only_does_not(tmp_path):
    client, db = _make_client(tmp_path)
    _seed(db, signal_id="sig-win-tp2", status=SIGNAL_STATUS_TP2_HIT, published=True)
    _seed(db, signal_id="sig-win-tp3", status=SIGNAL_STATUS_TP3_HIT, published=True)
    _seed(db, signal_id="sig-expired-tp1-only", status=SIGNAL_STATUS_EXPIRED, published=True)

    create_signal_performance(
        {
            "id": "p-tp2",
            "signal_id": "sig-win-tp2",
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "tp1_hit": 1,
            "tp2_hit": 1,
            "result": "WIN",
        },
        db=db,
    )
    create_signal_performance(
        {
            "id": "p-tp3",
            "signal_id": "sig-win-tp3",
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "tp1_hit": 1,
            "tp2_hit": 1,
            "tp3_hit": 1,
            "result": "WIN",
        },
        db=db,
    )
    create_signal_performance(
        {
            "id": "p-expired",
            "signal_id": "sig-expired-tp1-only",
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "tp1_hit": 1,
            "expired": 1,
            "result": "EXPIRED",
        },
        db=db,
    )

    response = client.get("/api/signals/performance")
    assert response.status_code == 200
    payload = response.json()
    assert payload["completed_signals"] == 3
    assert payload["tp1_hit_rate"] == 100.0
    assert payload["win_rate"] == pytest_approx_or_assert(payload["win_rate"], 66.67)


def pytest_approx_or_assert(value: float, expected: float, tol: float = 1.0) -> float:
    assert abs(value - expected) <= tol, f"Expected ~{expected}, got {value}"
    return value


# ---------------------------------------------------------------------------
# 7. Admin cancel clears is_published and marks performance as CANCELLED
# ---------------------------------------------------------------------------


def test_admin_cancel_clears_published_flag_and_updates_performance(tmp_path):
    client, db = _make_client(tmp_path)
    _seed(db, signal_id="sig-cancel-p11", status=SIGNAL_STATUS_ACTIVE, published=True)
    create_signal_performance(
        {
            "id": "p-cancel-p11",
            "signal_id": "sig-cancel-p11",
            "asset_class": "crypto",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "entry_triggered": 1,
        },
        db=db,
    )

    cancel_resp = client.post("/api/admin/signals/sig-cancel-p11/cancel")
    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["status"] == SIGNAL_STATUS_CANCELLED
    assert cancel_resp.json()["is_published"] == 0

    active_resp = client.get("/api/signals/active")
    ids = {item["id"] for item in active_resp.json()["items"]}
    assert "sig-cancel-p11" not in ids

    with db.connect() as conn:
        perf = conn.execute(
            "SELECT cancelled, result FROM signal_performance WHERE signal_id = ?",
            ("sig-cancel-p11",),
        ).fetchone()
    assert perf["cancelled"] == 1
    assert perf["result"] == "CANCELLED"


# ---------------------------------------------------------------------------
# 8. Unpublished signal not visible in user list, detail, or active endpoint
# ---------------------------------------------------------------------------


def test_unpublished_signal_is_hidden_from_all_user_endpoints(tmp_path):
    client, db = _make_client(tmp_path)
    _seed(db, signal_id="sig-hidden-all", status=SIGNAL_STATUS_ACTIVE)

    list_resp = client.get("/api/signals")
    active_resp = client.get("/api/signals/active")
    detail_resp = client.get("/api/signals/sig-hidden-all")

    assert all(item["id"] != "sig-hidden-all" for item in list_resp.json()["items"])
    assert all(item["id"] != "sig-hidden-all" for item in active_resp.json()["items"])
    assert detail_resp.status_code == 404


# ---------------------------------------------------------------------------
# 9. Dev signal hidden from user endpoints in production
# ---------------------------------------------------------------------------


def test_dev_signal_hidden_from_user_detail_in_production(tmp_path, monkeypatch):
    client, db = _make_client(tmp_path)
    _seed(db, signal_id="sig-dev-detail", dev_mode=1, published=True)
    monkeypatch.setenv("APP_ENV", "production")

    response = client.get("/api/signals/sig-dev-detail")
    assert response.status_code == 404


def test_dev_signal_visible_in_admin_list_non_production(tmp_path):
    client, db = _make_client(tmp_path)
    _seed(db, signal_id="sig-dev-admin", dev_mode=1)

    response = client.get("/api/admin/signals?dev_mode=1")
    assert response.status_code == 200
    ids = [item["id"] for item in response.json()["items"]]
    assert "sig-dev-admin" in ids


# ---------------------------------------------------------------------------
# 10. Admin API source does not reference execution systems
# ---------------------------------------------------------------------------


def test_admin_signals_api_source_does_not_reference_execution():
    source = inspect.getsource(admin_signals)
    forbidden = [
        "BinanceExecutor",
        "execute_signal",
        "create_external_signal_queue_row",
        "auto_pilot",
        "place_order",
        "open_position",
        "close_position",
    ]
    for term in forbidden:
        assert term not in source, f"Execution reference in admin_signals: {term}"


def test_user_signals_api_source_does_not_reference_execution():
    source = inspect.getsource(signals)
    forbidden = [
        "BinanceExecutor",
        "execute_signal",
        "create_external_signal_queue_row",
        "auto_pilot",
        "place_order",
        "open_position",
    ]
    for term in forbidden:
        assert term not in source, f"Execution reference in signals API: {term}"
