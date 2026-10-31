"""The bot backend's startup and shutdown run through a lifespan (F6, Starlette 1.x).

Starlette 1.x has no ``@app.on_event``. ``app.main`` registers its hooks with
``@_on_startup`` / ``@_on_shutdown`` and ``_lifespan`` runs them. These tests pin
what on_event guaranteed and the runtime relies on:

* the hooks run in the order they were registered, startup and shutdown alike
  (shutdown is NOT reversed: the run manager releases the runtime lease first);
* a startup hook that raises refuses startup: nothing after it runs, no shutdown
  hook runs, and the server (here a TestClient context) does not come up;
* a shutdown hook that raises stops the hooks after it;
* a TestClient used as a context manager triggers both phases.
"""
from __future__ import annotations

import inspect
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.main as main

STARTUP_ORDER = [
    "_startup_open_runtime_session",
    "_startup_validate_config",
    "_startup_stop_file_watcher",
    "_startup_runtime_supervisor",
    "_startup_run_manager",
    "_startup_notifications",
    "_startup_background_jobs",
]
SHUTDOWN_ORDER = [
    "_shutdown_run_manager",
    "_shutdown_notifications",
    "_shutdown_cati_simulation",
    "_shutdown_signal_scheduler",
    "on_shutdown",
]


def test_hooks_are_registered_in_the_order_on_event_used():
    assert [hook.__name__ for hook in main._STARTUP_HOOKS] == STARTUP_ORDER
    assert [hook.__name__ for hook in main._SHUTDOWN_HOOKS] == SHUTDOWN_ORDER
    assert all(inspect.iscoroutinefunction(hook) for hook in main._STARTUP_HOOKS + main._SHUTDOWN_HOOKS)


def test_no_on_event_handler_is_left_behind():
    source = Path(inspect.getsourcefile(main)).read_text(encoding="utf-8")
    assert "@app.on_event(" not in source
    assert "add_event_handler(" not in source
    assert not getattr(main.app.router, "on_startup", None) and not getattr(main.app.router, "on_shutdown", None)


def _recording(calls, name, exc=None):
    async def hook():
        calls.append(name)
        if exc is not None:
            raise exc
    hook.__name__ = name
    return hook


def test_testclient_context_runs_startup_then_shutdown_in_order(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(main, "_STARTUP_HOOKS", [_recording(calls, "s1"), _recording(calls, "s2")])
    monkeypatch.setattr(main, "_SHUTDOWN_HOOKS", [_recording(calls, "d1"), _recording(calls, "d2")])

    with TestClient(main.app):
        assert calls == ["s1", "s2"]
    assert calls == ["s1", "s2", "d1", "d2"]


def test_a_failing_startup_hook_refuses_startup_and_runs_nothing_after_it(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(main, "_STARTUP_HOOKS", [
        _recording(calls, "s1"), _recording(calls, "refuse", RuntimeError("refused")), _recording(calls, "s3")])
    monkeypatch.setattr(main, "_SHUTDOWN_HOOKS", [_recording(calls, "d1")])

    with pytest.raises(RuntimeError, match="refused"):
        with TestClient(main.app):
            pytest.fail("the application must not come up")
    assert calls == ["s1", "refuse"]


def test_a_failing_shutdown_hook_stops_the_ones_after_it(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(main, "_STARTUP_HOOKS", [])
    monkeypatch.setattr(main, "_SHUTDOWN_HOOKS", [
        _recording(calls, "d1"), _recording(calls, "boom", RuntimeError("shutdown failed")), _recording(calls, "d3")])

    with pytest.raises(RuntimeError, match="shutdown failed"):
        with TestClient(main.app):
            pass
    assert calls == ["d1", "boom"]


def test_the_live_mode_safety_refusal_still_refuses_startup(monkeypatch):
    """The real ``_startup_validate_config``: live mode with a failing safety check never serves."""
    from types import SimpleNamespace

    from app.core import safety_startup

    calls: list[str] = []
    monkeypatch.setattr(safety_startup, "run_startup_safety_check",
                        lambda settings, mode="paper": SimpleNamespace(all_pass=False))
    monkeypatch.setattr(main.settings, "EXECUTION_MODE", "live")
    monkeypatch.setattr(main, "_STARTUP_HOOKS", [main._startup_validate_config, _recording(calls, "after")])
    monkeypatch.setattr(main, "_SHUTDOWN_HOOKS", [_recording(calls, "shutdown")])

    with pytest.raises(RuntimeError, match="Safety startup check FAILED in live mode"):
        with TestClient(main.app):
            pytest.fail("the application must not come up")
    assert calls == []
