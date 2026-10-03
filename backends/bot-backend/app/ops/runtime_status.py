"""Read-only runtime liveness from the canonical launcher preflight and session."""
from datetime import datetime, timezone

from app.ops.runtime_preflight import preflight, PreflightStatus


def runtime_process_status(db):
    inspected = preflight(db=db)
    state, reason = "STOPPED", "NO_ACTIVE_RUNTIME"
    session_valid = False
    if inspected.lease_session_id:
        try:
            with db.connect() as conn:
                row = conn.execute("SELECT pid, status, started_at FROM runtime_sessions "
                                   "WHERE runtime_session_id=?", (inspected.lease_session_id,)).fetchone()
            session_valid = bool(row and row["status"] == "RUNNING"
                                 and row["pid"] == inspected.lease_pid)
            if session_valid:
                import psutil
                started = datetime.fromisoformat(row["started_at"].replace("Z", "+00:00"))
                if started.tzinfo is None:
                    started = started.replace(tzinfo=timezone.utc)
                session_valid = psutil.Process(inspected.lease_pid).create_time() <= started.timestamp() + 2
        except Exception:
            session_valid = False
    if inspected.lease_pid and inspected.lease_owner_alive is False:
        reason = "RUNTIME_OWNER_PROCESS_GONE"
    elif inspected.status == PreflightStatus.ALREADY_RUNNING and session_valid and (
        inspected.lease_owner_alive is True and inspected.lease_stale is False
        and inspected.port_pid == inspected.lease_pid
    ):
        state, reason = "RUNNING", "CANONICAL_RUNTIME_SESSION_LEASE_HEARTBEAT_VALID"
    elif inspected.lease_pid or inspected.status not in (PreflightStatus.READY,):
        state, reason = "STALE", "RUNTIME_LIVENESS_UNCONFIRMED"
    return {"state": state, "active": state == "RUNNING", "reason": reason,
            "heartbeat_age_seconds": inspected.lease_heartbeat_age_seconds,
            "session_valid": session_valid, "preflight_status": inspected.status.value}
