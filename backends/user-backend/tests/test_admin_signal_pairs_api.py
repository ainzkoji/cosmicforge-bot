from __future__ import annotations

import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import admin_signal_pairs  # noqa: E402
from app.core.deps import require_admin  # noqa: E402
from app.core.security import create_access_token  # noqa: E402
from shared_lib.persistence.db import DB, utc_now_iso  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402
from shared_lib.persistence.signals import (  # noqa: E402
    complete_signal_scan_run,
    create_signal_scan_result,
    create_signal_scan_run,
    get_eligible_signal_symbols,
    upsert_signal_pair,
    upsert_signal_pair_metrics,
)


def _make_client(tmp_path: Path, *, admin: bool = True) -> tuple[TestClient, DB]:
    db_path = tmp_path / "admin_signal_pairs_api.db"
    migrate(str(db_path))
    db = DB(path=str(db_path))
    app = FastAPI()
    app.include_router(admin_signal_pairs.router, prefix="/api")
    app.dependency_overrides[admin_signal_pairs._get_db] = lambda: db
    if admin:
        app.dependency_overrides[require_admin] = lambda: {"id": "admin-test", "role": "admin"}
    return TestClient(app), db


def _seed_pair(db: DB, *, symbol: str = "DOTUSDT", is_safe: int = 1, blacklisted: int = 0) -> None:
    now = utc_now_iso()
    upsert_signal_pair(
        {
            "symbol": symbol,
            "exchange": "binance_futures",
            "asset_class": "crypto",
            "quote_asset": "USDT",
            "contract_type": "PERPETUAL",
            "tier": "TIER_2",
            "enabled": 1,
            "whitelisted": 0,
            "blacklisted": blacklisted,
            "blacklist_reason": "test blacklist" if blacklisted else None,
            "discovered_at": now,
            "last_seen_at": now,
            "created_at": now,
            "updated_at": now,
        },
        db=db,
    )
    upsert_signal_pair_metrics(
        {
            "symbol": symbol,
            "exchange": "binance_futures",
            "quote_volume_24h": 100_000_000,
            "spread_percent": 0.04,
            "bid_price": 10.0,
            "ask_price": 10.004,
            "candle_count": 240,
            "atr_percent": 0.01,
            "volatility_score": 80,
            "liquidity_score": 80,
            "spread_score": 90,
            "reliability_score": 85,
            "is_safe": is_safe,
            "unsafe_reason": None if is_safe else "LOW_VOLUME",
            "last_updated": now,
        },
        db=db,
    )


def test_admin_can_list_pairs_and_metrics(tmp_path):
    client, db = _make_client(tmp_path)
    _seed_pair(db, symbol="DOTUSDT")

    pairs = client.get("/api/admin/signals/pairs?search=DOT")
    metrics = client.get("/api/admin/signals/pairs/metrics?symbol=DOTUSDT")

    assert pairs.status_code == 200
    assert pairs.json()["items"][0]["symbol"] == "DOTUSDT"
    assert pairs.json()["count"] == 1
    assert metrics.status_code == 200
    assert metrics.json()["items"][0]["is_safe"] == 1
    assert metrics.json()["items"][0]["tier"] == "TIER_2"


def test_admin_can_enable_disable_blacklist_and_whitelist_pair(tmp_path):
    client, db = _make_client(tmp_path)
    _seed_pair(db, symbol="NEARUSDT")

    disabled = client.post("/api/admin/signals/pairs/NEARUSDT/disable")
    enabled = client.post("/api/admin/signals/pairs/NEARUSDT/enable")
    blacklisted = client.post("/api/admin/signals/pairs/NEARUSDT/blacklist", json={"reason": "Too risky"})
    whitelisted = client.post("/api/admin/signals/pairs/NEARUSDT/whitelist")

    assert disabled.status_code == 200
    assert disabled.json()["enabled"] == 0
    assert enabled.status_code == 200
    assert enabled.json()["enabled"] == 1
    assert blacklisted.status_code == 200
    assert blacklisted.json()["blacklisted"] == 1
    assert blacklisted.json()["enabled"] == 0
    assert blacklisted.json()["blacklist_reason"] == "Too risky"
    assert whitelisted.status_code == 200
    assert whitelisted.json()["whitelisted"] == 1
    assert whitelisted.json()["blacklisted"] == 1
    assert whitelisted.json()["warning"] == "BLACKLIST_OVERRIDES_WHITELIST"
    assert get_eligible_signal_symbols(db=db) == []


def test_admin_refresh_discovery_uses_runner_and_does_not_generate_signals(tmp_path):
    client, db = _make_client(tmp_path)

    def fake_runner(request, injected_db):
        assert injected_db is db
        assert request.quote_asset == "USDT"
        return {
            "scan_run_id": "sigscan-test",
            "symbols_discovered": 2,
            "symbols_eligible": 1,
            "symbols_skipped": 1,
            "metrics_updated": 1,
            "errors": [],
        }

    client.app.dependency_overrides[admin_signal_pairs._get_discovery_runner] = lambda: fake_runner

    response = client.post("/api/admin/signals/pairs/refresh", json={"validate_candles": False})

    assert response.status_code == 200
    assert response.json()["symbols_eligible"] == 1
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) AS count FROM trading_signals").fetchone()["count"] == 0
        assert conn.execute("SELECT COUNT(*) AS count FROM signal_candidates").fetchone()["count"] == 0


def test_admin_can_list_scan_runs_and_view_results(tmp_path):
    client, db = _make_client(tmp_path)
    scan_run_id = create_signal_scan_run("PAIR_DISCOVERY", db=db)
    create_signal_scan_result(
        {
            "scan_run_id": scan_run_id,
            "symbol": "BADUSDT",
            "was_skipped": 1,
            "skip_reason": "LOW_VOLUME",
        },
        db=db,
    )
    complete_signal_scan_run(scan_run_id, {"symbols_discovered": 1, "symbols_eligible": 0}, db=db)

    runs = client.get("/api/admin/signals/scan-runs")
    detail = client.get(f"/api/admin/signals/scan-runs/{scan_run_id}")

    assert runs.status_code == 200
    assert runs.json()["items"][0]["id"] == scan_run_id
    assert detail.status_code == 200
    assert detail.json()["scan_run"]["id"] == scan_run_id
    assert detail.json()["results"][0]["skip_reason"] == "LOW_VOLUME"


def test_normal_user_and_unauthenticated_user_cannot_access_pair_apis(tmp_path):
    unauth_client, _ = _make_client(tmp_path / "unauth", admin=False)
    user_client, db = _make_client(tmp_path / "user", admin=False)
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

    unauth = unauth_client.get("/api/admin/signals/pairs")
    normal_user = user_client.get("/api/admin/signals/pairs", headers={"Authorization": f"Bearer {token}"})

    assert unauth.status_code == 401
    assert normal_user.status_code == 401
