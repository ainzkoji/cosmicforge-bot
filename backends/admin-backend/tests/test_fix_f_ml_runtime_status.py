"""FIX-F: Backend tests for GET /api/admin/ml/runtime-status.

7 tests covering:
  1. Endpoint requires admin auth (401 without token)
  2. Safe defaults when no snapshot table exists
  3. safe_state_confirmed=True when ml_enabled=False in snapshot
  4. safe_state_confirmed=False when ml_enabled=True in snapshot (edge-case guard)
  5. All 14+ required fields are present in the response
  6. Document 1 Phase I readiness checklist structure is always present
  7. model_load_error is surfaced when present in the overview snapshot

CRITICAL: None of these tests set ML_ENABLED=True or ML_SHADOW_MODE=False.
Test 4 only *reads* a snapshot that contains ml_enabled=True to verify the
admin dashboard correctly surfaces the dangerous state — it does NOT change
any config or enable live scoring.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from jose import jwt

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "admin-backend"))
sys.path.insert(0, str(ROOT / "backends" / "shared"))

from app.api import health as health_api  # noqa: E402
from app.core.config import settings  # noqa: E402
from app.core import deps as deps_api  # noqa: E402
from app.main import app  # noqa: E402
from app.persistence.db import AdminDB  # noqa: E402


# ── Helpers ──────────────────────────────────────────────────────────────────

def _admin_token(admin_id: str = "admin-test") -> str:
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {
            "exp": now + timedelta(minutes=15),
            "nbf": now,
            "iat": now,
            "iss": "cosmicforge-admin-backend",
            "aud": "admin-portal",
            "sub": admin_id,
            "type": "admin_access",
            "role": "admin",
        },
        settings.SECRET_KEY,
        algorithm=settings.ALGORITHM,
    )


def _admin_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_admin_token()}"}


def _make_minimal_db(tmp_path: Path, *, overview_override: dict | None = None) -> AdminDB:
    """Create an AdminDB with only the tables required for the runtime-status endpoint."""
    db_path = tmp_path / "fix_f_test.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE admins (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            full_name TEXT,
            role TEXT,
            is_superuser INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
        """
    )
    conn.execute(
        "INSERT INTO admins (id, email, role, is_superuser, is_active) "
        "VALUES ('admin-test', 'admin@example.com', 'admin', 1, 1)"
    )
    conn.commit()
    conn.close()

    db = AdminDB(str(db_path))
    if overview_override is not None:
        _write_snapshot_with_overview(db, overview_override)
    return db


def _write_snapshot_with_overview(db: AdminDB, overview: dict) -> None:
    """Write an admin_ml_dashboard_snapshot row so the endpoint reads real data.

    Uses a raw sqlite3 connection with explicit commit because AdminDB.connect()
    is read-oriented and does not auto-commit writes.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    training_gate = {
        "total_linked_completed_trades": 2,
        "required_trades": 200,
        "wins": 1,
        "required_wins": 50,
        "losses": 1,
        "breakeven_trades": 0,
        "excluded_open_positions": 0,
        "trades_with_full_feature_coverage": 2,
        "trades_missing_critical_features": 0,
        "current_win_rate": 50.0,
        "feature_coverage_pct": 100.0,
        "linkage_healthy": True,
        "label_distribution_single_class": False,
        "training_ready": False,
        "status": "not_ready",
    }
    dataset_status = {
        "fully_usable_rows": 314,
        "linked_trade_count": 2,
        "dropped_rows": 0,
        "dropped_row_reasons": [],
        "feature_completeness_status": "healthy",
        "label_distribution": {"wins": 1, "losses": 1, "breakevens": 0, "single_class": False},
        "last_dataset_build_time": now_iso,
        "last_dataset_path": "models/datasets/v2_latest.parquet",
        "rebuild_dataset_allowed": True,
    }
    # Use raw sqlite3 so we can commit writes (AdminDB.connect() is read-oriented)
    conn = sqlite3.connect(str(db.path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_ml_dashboard_snapshot (
                snapshot_key TEXT PRIMARY KEY,
                generated_at TEXT NOT NULL,
                overview_json TEXT,
                training_gate_json TEXT,
                dataset_status_json TEXT,
                dashboard_json TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO admin_ml_dashboard_snapshot
                (snapshot_key, generated_at, overview_json, training_gate_json, dataset_status_json)
            VALUES ('latest', ?, ?, ?, ?)
            """,
            (now_iso, json.dumps(overview), json.dumps(training_gate), json.dumps(dataset_status)),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def client_no_snapshot(tmp_path: Path):
    db = _make_minimal_db(tmp_path)
    app.dependency_overrides[health_api.get_db] = lambda: db
    app.dependency_overrides[deps_api.get_db] = lambda: db
    with TestClient(app) as tc:
        yield tc
    app.dependency_overrides.clear()


@pytest.fixture
def client_ml_disabled(tmp_path: Path):
    overview = {
        "ml_enabled": False,
        "ml_shadow_mode": True,
        "ml_mode": "shadow",
        "current_ml_status": "not_ready",
        "current_model_version": None,
        "model_artifact_path": None,
        "encoder_path": None,
        "metadata_path": None,
        "model_load_error": None,
        "last_model_load_time": None,
        "last_score_timestamp": None,
        "last_bot_restart_time": None,
        "current_threshold": None,
        "current_hard_block_floor": None,
        "contract_version": "v2",
    }
    db = _make_minimal_db(tmp_path, overview_override=overview)
    app.dependency_overrides[health_api.get_db] = lambda: db
    app.dependency_overrides[deps_api.get_db] = lambda: db
    with TestClient(app) as tc:
        yield tc
    app.dependency_overrides.clear()


@pytest.fixture
def client_ml_enabled_in_snapshot(tmp_path: Path):
    """Snapshot contains ml_enabled=True.  Used only to verify the dashboard
    surfaces the dangerous state — ML is NOT actually being enabled here."""
    overview = {
        "ml_enabled": True,      # dangerous — snapshot from a hypothetical bad state
        "ml_shadow_mode": False,  # also dangerous
        "ml_mode": "active",
        "current_ml_status": "ready_for_live_promotion",
        "current_model_version": "entry_quality_v1.0",
        "model_artifact_path": "models/artifacts/v1.0/model.pkl",
        "encoder_path": "models/artifacts/v1.0/encoder.pkl",
        "metadata_path": "models/artifacts/v1.0/metadata.json",
        "model_load_error": None,
        "last_model_load_time": "2026-06-10T10:00:00+00:00",
        "last_score_timestamp": "2026-06-10T10:05:00+00:00",
        "last_bot_restart_time": "2026-06-10T09:00:00+00:00",
        "current_threshold": 0.55,
        "current_hard_block_floor": 0.50,
        "contract_version": "v2",
    }
    db = _make_minimal_db(tmp_path, overview_override=overview)
    app.dependency_overrides[health_api.get_db] = lambda: db
    app.dependency_overrides[deps_api.get_db] = lambda: db
    with TestClient(app) as tc:
        yield tc
    app.dependency_overrides.clear()


@pytest.fixture
def client_load_error_in_snapshot(tmp_path: Path):
    overview = {
        "ml_enabled": False,
        "ml_shadow_mode": True,
        "ml_mode": "disabled",
        "current_ml_status": "not_ready",
        "current_model_version": "entry_quality_v1.0",
        "model_artifact_path": "models/artifacts/v1.0/model.pkl",
        "encoder_path": "models/artifacts/v1.0/encoder.pkl",
        "metadata_path": "models/artifacts/v1.0/metadata.json",
        "model_load_error": "FileNotFoundError: model.pkl not found at expected path",
        "last_model_load_time": None,
        "last_score_timestamp": None,
        "last_bot_restart_time": "2026-06-10T09:00:00+00:00",
        "current_threshold": None,
        "current_hard_block_floor": None,
        "contract_version": "v2",
    }
    db = _make_minimal_db(tmp_path, overview_override=overview)
    app.dependency_overrides[health_api.get_db] = lambda: db
    app.dependency_overrides[deps_api.get_db] = lambda: db
    with TestClient(app) as tc:
        yield tc
    app.dependency_overrides.clear()


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_runtime_status_requires_admin(client_no_snapshot: TestClient):
    """Endpoint must reject requests without a valid admin token."""
    response = client_no_snapshot.get("/api/admin/ml/runtime-status")
    assert response.status_code == 401, (
        f"Expected 401 Unauthorized for unauthenticated request, got {response.status_code}"
    )


def test_runtime_status_returns_safe_defaults_when_no_snapshot(client_no_snapshot: TestClient):
    """When no snapshot exists the endpoint must return safe defaults:
    ml_enabled=False, safe_state_confirmed=True, phase_i_readiness present."""
    response = client_no_snapshot.get(
        "/api/admin/ml/runtime-status", headers=_admin_headers()
    )
    assert response.status_code == 200, f"Unexpected status {response.status_code}"
    payload = response.json()

    assert payload["ml_enabled"] is False, "ml_enabled must default to False (safe)"
    assert payload["safe_state_confirmed"] is True, "safe_state_confirmed must be True when ml_enabled=False"
    assert "phase_i_readiness" in payload, "phase_i_readiness checklist must always be present"
    assert isinstance(payload["phase_i_readiness"]["checklist"], list), "checklist must be a list"
    assert payload["snapshot_missing"] is True, "snapshot_missing must be True when no table exists"


def test_runtime_status_safe_state_confirmed_true_when_ml_disabled(client_ml_disabled: TestClient):
    """Snapshot explicitly has ml_enabled=False → safe_state_confirmed=True."""
    response = client_ml_disabled.get(
        "/api/admin/ml/runtime-status", headers=_admin_headers()
    )
    assert response.status_code == 200
    payload = response.json()

    assert payload["ml_enabled"] is False
    assert payload["ml_shadow_mode"] is True
    assert payload["safe_state_confirmed"] is True
    assert "ML is disabled" in payload["safe_state_note"], (
        f"safe_state_note should confirm ML is disabled, got: {payload['safe_state_note']!r}"
    )
    assert payload["snapshot_missing"] is False


def test_runtime_status_safe_state_false_when_ml_enabled_in_snapshot(
    client_ml_enabled_in_snapshot: TestClient,
):
    """When the snapshot contains ml_enabled=True, the dashboard must surface
    safe_state_confirmed=False so the admin sees a visible warning.

    NOTE: This test does NOT enable ML in any running process. It only writes a
    snapshot row with ml_enabled=True to verify the endpoint reads and surfaces
    the dangerous state correctly — the safety check that matters in production.
    """
    response = client_ml_enabled_in_snapshot.get(
        "/api/admin/ml/runtime-status", headers=_admin_headers()
    )
    assert response.status_code == 200
    payload = response.json()

    assert payload["ml_enabled"] is True, (
        "Snapshot had ml_enabled=True; endpoint must surface this faithfully"
    )
    assert payload["safe_state_confirmed"] is False, (
        "safe_state_confirmed must be False when ml_enabled=True"
    )
    assert "WARNING" in payload["safe_state_note"], (
        f"safe_state_note should contain a WARNING when ML is enabled, got: {payload['safe_state_note']!r}"
    )


def test_runtime_status_has_all_required_fields(client_ml_disabled: TestClient):
    """Response must contain all 14+ required FIX-F fields."""
    required_fields = [
        # Safety
        "ml_enabled",
        "ml_shadow_mode",
        "safe_state_confirmed",
        "safe_state_note",
        # Runtime identity
        "ml_mode",
        "current_ml_status",
        "model_version",
        "contract_version",
        # Artifact paths
        "model_artifact_path",
        "encoder_path",
        "metadata_path",
        # Load state
        "model_load_error",
        "last_model_load_time",
        "last_score_timestamp",
        "last_bot_restart_time",
        # Threshold
        "current_threshold",
        "current_hard_block_floor",
        # Checklist
        "phase_i_readiness",
        # Snapshot metadata
        "snapshot_missing",
        "snapshot_stale",
        "generated_at",
        "source_note",
    ]
    response = client_ml_disabled.get(
        "/api/admin/ml/runtime-status", headers=_admin_headers()
    )
    assert response.status_code == 200
    payload = response.json()

    missing = [f for f in required_fields if f not in payload]
    assert not missing, (
        f"Missing required fields from runtime-status response: {missing}"
    )
    assert len(required_fields) >= 14, "At least 14 required fields (FIX-F spec)"


def test_runtime_status_document1_checklist_always_present(client_ml_disabled: TestClient):
    """The phase_i_readiness checklist must always be present with the correct structure."""
    response = client_ml_disabled.get(
        "/api/admin/ml/runtime-status", headers=_admin_headers()
    )
    assert response.status_code == 200
    payload = response.json()

    phase_i = payload["phase_i_readiness"]
    assert isinstance(phase_i, dict), "phase_i_readiness must be a dict"
    assert "checklist" in phase_i, "checklist key must be present"
    assert "met_count" in phase_i, "met_count key must be present"
    assert "total_count" in phase_i, "total_count key must be present"
    assert "overall_ready" in phase_i, "overall_ready key must be present"
    assert "overall_note" in phase_i, "overall_note key must be present"

    checklist = phase_i["checklist"]
    assert isinstance(checklist, list), "checklist must be a list"
    assert len(checklist) > 0, "checklist must have at least one item"

    # Each item must have the required keys
    for item in checklist:
        for key in ("id", "label", "met", "detail", "category"):
            assert key in item, f"checklist item missing key {key!r}: {item}"
        assert isinstance(item["met"], bool), f"met must be bool, got {type(item['met'])}"

    # FIX-F: ML visibility item should be met=True (this task)
    fix_f_item = next((i for i in checklist if i["id"] == "fix_f_ml_visibility"), None)
    assert fix_f_item is not None, "fix_f_ml_visibility checklist item must be present"
    assert fix_f_item["met"] is True, "FIX-F: ML visibility must be marked as deployed"


def test_runtime_status_model_load_error_surfaces(client_load_error_in_snapshot: TestClient):
    """model_load_error from the snapshot must be surfaced in the response."""
    response = client_load_error_in_snapshot.get(
        "/api/admin/ml/runtime-status", headers=_admin_headers()
    )
    assert response.status_code == 200
    payload = response.json()

    assert payload["model_load_error"] is not None, (
        "model_load_error should be non-null when snapshot contains a load error"
    )
    assert "FileNotFoundError" in payload["model_load_error"], (
        f"model_load_error should contain the error message, got: {payload['model_load_error']!r}"
    )
    # Safety: ml_enabled must still be False (load error should not imply ML is enabled)
    assert payload["ml_enabled"] is False
    assert payload["safe_state_confirmed"] is True
