from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from app.ops.runtime_preflight import PreflightResult, PreflightStatus
from app.ops import runtime_status
from app.api import auto_pilot
from app.models.bot_instance_models import BotInstance


@pytest.mark.parametrize("mode,expected", [("healthy", "RUNNING"), ("no_lease", "STOPPED"),
    ("dead", "STOPPED"), ("stale", "STALE"), ("missing_session", "STALE"),
    ("session_stopped", "STALE"), ("pid_reused", "STALE"), ("port_conflict", "STALE")])
def test_runtime_status_requires_process_session_lease_heartbeat(mode, expected, monkeypatch):
    row = {"pid": 42, "status": "RUNNING", "started_at": datetime.now(timezone.utc).isoformat()}
    conn = Mock()
    conn.execute.return_value.fetchone.return_value = None if mode == "missing_session" else row
    if mode == "session_stopped":
        row["status"] = "STOPPED"
    @contextmanager
    def connect():
        yield conn
    result = PreflightResult(PreflightStatus.ALREADY_RUNNING, "test", lease_pid=42,
        lease_owner_alive=mode != "dead", lease_stale=mode == "stale", port_pid=43 if mode == "port_conflict" else 42,
        lease_session_id="session", lease_heartbeat_age_seconds=100 if mode == "stale" else 1)
    if mode == "no_lease":
        result = PreflightResult(PreflightStatus.READY, "none")
    monkeypatch.setattr(runtime_status, "preflight", lambda **kw: result)
    import psutil
    monkeypatch.setattr(psutil, "Process", lambda pid: SimpleNamespace(create_time=lambda:
        datetime.now(timezone.utc).timestamp() + (60 if mode == "pid_reused" else -60)))
    status = runtime_status.runtime_process_status(SimpleNamespace(connect=connect))
    assert status["state"] == expected
    assert status["active"] is (expected == "RUNNING")


def test_auto_pilot_status_and_controls_exclude_historical_records(monkeypatch):
    def bot(identity, strategy, status):
        return BotInstance(id=identity, user_id="u", broker_account_id="a", market_type="CRYPTO",
            strategy_id=strategy, strategy_version="1", risk_level="balanced", status=status, capital_allocation=100)
    bots = [bot("cati-active", "cati", "active"), bot("cati-paused", "cati", "paused"),
            bot("old-active", "master_ensemble", "active"), bot("old-paused", "robust_ensemble", "paused"),
            bot("old-stopped", "sma_cross", "stopped")]
    for b in bots:
        b.last_run_id = None
    service = SimpleNamespace(get_user_bot_instances=lambda user: bots,
                              pause_bot_instance=Mock(), start_bot_instance=Mock())
    @contextmanager
    def connect():
        yield Mock()
    monkeypatch.setattr("shared_lib.persistence.db.DB", lambda: SimpleNamespace(connect=connect))
    status = auto_pilot.get_auto_pilot_status(user={"id": "u"}, service=service)
    assert [b.id for b in status.instances] == ["cati-active", "cati-paused"]
    assert status.active_count == status.paused_count == 1
    assert status.total_equity == 200
    auto_pilot.pause_auto_pilot(user={"id": "u"}, service=service)
    auto_pilot.resume_auto_pilot(user={"id": "u"}, service=service)
    service.pause_bot_instance.assert_called_once_with("cati-active")
    service.start_bot_instance.assert_called_once_with("cati-paused")
    assert len(bots) == 5 and bots[2].status == "active" and bots[3].status == "paused"
