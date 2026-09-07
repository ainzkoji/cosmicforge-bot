"""``last_error`` must describe the CURRENT failure, never a repaired one.

Observed contradiction that motivated this file:

    bot_health_reason_code = ERROR_STRATEGY_UNAVAILABLE
    last_error             = "Auto Pilot capital budget is missing..."

The capital problem had already been repaired.  Health advanced; ``last_error``
did not, because both writers used ``COALESCE(?, last_error)`` / "only write
when explicitly supplied".  Historical evidence is preserved append-only in
``bot_system_events``, so clearing the current-state column destroys nothing.
"""
from __future__ import annotations

import sqlite3
import uuid

import pytest

from app.core.bot_health import (
    ERROR_HEALTH_STATUSES,
    is_error_health_status,
    resolve_last_error,
)


# ── Semantics ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("status", sorted(ERROR_HEALTH_STATUSES))
def test_error_statuses_are_recognised(status):
    assert is_error_health_status(status)


@pytest.mark.parametrize("status", ["RUNNING", "WAITING_FOR_SETUP", "OK", "STARTING", None, ""])
def test_healthy_statuses_are_not_errors(status):
    assert not is_error_health_status(status)


def test_leaving_an_error_state_clears_last_error():
    assert resolve_last_error(
        status="WAITING_FOR_SETUP",
        message="No high-quality setup right now",
        explicit_last_error="Auto Pilot capital budget is missing",
    ) is None


def test_new_error_replaces_the_previous_error_message():
    assert resolve_last_error(
        status="ERROR",
        message="Cycle completed: ERROR_STRATEGY_UNAVAILABLE",
        reason_code="ERROR_STRATEGY_UNAVAILABLE",
        explicit_last_error=None,
    ) == "Cycle completed: ERROR_STRATEGY_UNAVAILABLE"


def test_explicit_error_wins_over_derived_message():
    assert resolve_last_error(
        status="ERROR_INITIALIZATION",
        message="short",
        explicit_last_error="ORCHESTRATOR_INITIALIZATION_FAILED: detail",
    ) == "ORCHESTRATOR_INITIALIZATION_FAILED: detail"


def test_error_without_message_falls_back_to_reason_code():
    assert resolve_last_error(status="ERROR", reason_code="BROKER_AUTH_FAILED") == "BROKER_AUTH_FAILED"


# ── Append-only history ──────────────────────────────────────────────────────


class _MemoryDB:
    """Minimal stand-in exposing the ``connect()`` context-manager contract."""

    def __init__(self):
        self._conn = sqlite3.connect(":memory:")
        self._conn.execute(
            """
            CREATE TABLE bot_system_events (
                event_id TEXT PRIMARY KEY, bot_instance_id TEXT, user_id TEXT,
                run_id TEXT, event_type TEXT NOT NULL, severity TEXT NOT NULL DEFAULT 'INFO',
                reason_code TEXT, message TEXT, details_json TEXT NOT NULL DEFAULT '{}',
                provenance TEXT NOT NULL DEFAULT 'RUNTIME', created_at TEXT NOT NULL
            )
            """
        )

    def connect(self):
        outer = self

        class _Ctx:
            def __enter__(self):
                return outer._conn

            def __exit__(self, *exc):
                return False

        return _Ctx()


def test_system_events_are_append_only_and_redact_secrets():
    from app.runner.system_events import record_bot_system_event

    db = _MemoryDB()
    bot_id = f"bot_{uuid.uuid4().hex[:8]}"

    first = record_bot_system_event(
        db, bot_instance_id=bot_id, event_type="EFFECTIVE_POLICY_REJECTED",
        severity="ERROR", reason_code="CAPITAL_BUDGET_REQUIRED",
        message="capital missing",
    )
    second = record_bot_system_event(
        db, bot_instance_id=bot_id, event_type="RUNNER_INITIALIZATION_FAILED",
        severity="ERROR", reason_code="ORCHESTRATOR_INITIALIZATION_FAILED",
        message="orchestrator failed",
        details={"api_secret": "super-secret", "cause_type": "AttributeError"},
    )
    assert first and second and first != second

    rows = db._conn.execute(
        "SELECT reason_code, details_json FROM bot_system_events ORDER BY created_at"
    ).fetchall()
    # The earlier error is still there: current-state clearing destroys no history.
    assert {r[0] for r in rows} == {
        "CAPITAL_BUDGET_REQUIRED",
        "ORCHESTRATOR_INITIALIZATION_FAILED",
    }
    assert "super-secret" not in " ".join(r[1] for r in rows)
    assert "***redacted***" in rows[1][1]


def test_event_write_failure_never_propagates():
    """Evidence recording must not be able to take down a trading runner."""
    from app.runner.system_events import record_bot_system_event

    class _BrokenDB:
        def connect(self):
            raise RuntimeError("database is locked")

    assert record_bot_system_event(
        _BrokenDB(), bot_instance_id="bot-x", event_type="ANY"
    ) is None
