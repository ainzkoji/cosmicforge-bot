"""Canonical pytest bootstrap for the bot-backend component.

Three jobs:

1. **Isolate the database before anything else happens.** A unique temporary
   SQLite database is created for the session and ``DATABASE_URL`` points at
   it *before any application import*, so ``DB()``, ``TraceRecorder``, the
   import-time migration and every module-level singleton resolve to it. A
   ``DATABASE_URL`` or ``DATABASE_ROLE`` inherited from the shell that names a
   runtime database stops the run here -- before a single evidence row can be
   written. (The suite previously wrote 9,339 decision rows into the paper
   database, 849 of them while the live runtime was running.)
2. Make ``app`` and ``shared_lib`` importable no matter which directory pytest
   was launched from, so the test interpreter sees exactly the tree the server
   imports.
3. Fail closed on real order submission. No test in this suite may reach a
   broker order endpoint; paper execution runs entirely inside PaperExecutor.
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

_BOT_BACKEND = Path(__file__).resolve().parent
_BACKENDS = _BOT_BACKEND.parent
_SHARED = _BACKENDS / "shared"

for _p in (_BOT_BACKEND, _SHARED):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import pytest  # noqa: E402

# Stdlib-only module: importing it does not import the application.
from shared_lib.persistence.test_isolation import (  # noqa: E402
    PROTECTED_ROLES,
    TEST_MODE_ENV,
    protection_reason,
    resolve_sqlite_url,
)

# ── 1. Database isolation, before any application import ───────────────────

_inherited_url = os.environ.get("DATABASE_URL")
_inherited_role = os.environ.get("DATABASE_ROLE")
_violations: list[str] = []
if _inherited_role and _inherited_role.strip().lower() in PROTECTED_ROLES:
    _violations.append(f"DATABASE_ROLE={_inherited_role!r} is a runtime role")
if _inherited_url:
    _inherited_reason = protection_reason(resolve_sqlite_url(_inherited_url))
    if _inherited_reason:
        _violations.append(f"DATABASE_URL={_inherited_url!r}: {_inherited_reason}")
if _violations:
    raise pytest.UsageError(
        "[TEST_DATABASE_ISOLATION_VIOLATION] pytest was started with a runtime "
        "database in its environment: " + "; ".join(_violations) + ". Refusing to "
        "run: tests use a per-session temporary database and must never write "
        "paper, live or canonical evidence. Unset these variables and retry."
    )

TEST_DATABASE_DIR = tempfile.mkdtemp(prefix="cosmicforge_pytest_")
TEST_DATABASE_PATH = os.path.join(TEST_DATABASE_DIR, "test_session.db")
TEST_DATABASE_ROLE = "test"

os.environ["DATABASE_URL"] = "sqlite:///" + TEST_DATABASE_PATH.replace("\\", "/")
os.environ["DATABASE_ROLE"] = TEST_DATABASE_ROLE
os.environ["ENVIRONMENT_NAME"] = "test"
os.environ["COSMICFORGE_TEST_DATABASE_PATH"] = TEST_DATABASE_PATH
os.environ[TEST_MODE_ENV] = "1"
# Tests never operate against real capital.
os.environ.setdefault("EXECUTION_MODE", "paper")


def pytest_report_header(config):
    return [
        f"TEST_DATABASE_PATH={TEST_DATABASE_PATH}",
        f"TEST_DATABASE_ROLE={TEST_DATABASE_ROLE}",
    ]


def pytest_sessionstart(session):
    """Prove the isolation holds before the first test runs."""
    from shared_lib.persistence.db import DB

    resolved = os.path.normcase(os.path.abspath(DB().path))
    expected = os.path.normcase(os.path.abspath(TEST_DATABASE_PATH))
    if resolved != expected:
        raise pytest.UsageError(
            f"[TEST_DATABASE_ISOLATION_VIOLATION] DB() resolved to {resolved}, "
            f"not the session test database {expected}"
        )
    # Printed even under -q (report headers are not), so every run states
    # which database it used before the first test executes.
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_line(f"TEST_DATABASE_PATH={TEST_DATABASE_PATH}")
        reporter.write_line(f"TEST_DATABASE_ROLE={TEST_DATABASE_ROLE}")


def pytest_unconfigure(config):
    shutil.rmtree(TEST_DATABASE_DIR, ignore_errors=True)


@pytest.fixture(autouse=True)
def _reset_cached_trace_recorder():
    """No test inherits a TraceRecorder bound to another test's database."""
    yield
    try:
        from shared_lib.persistence.trace_recorder import reset_trace_recorder

        reset_trace_recorder()
    except Exception:
        pass


# ── 3. Live order transport guard ────────────────────────────────────────────


class LiveOrderSubmissionBlocked(AssertionError):
    """Raised when a test attempts to submit an order to a real broker."""


# Path fragments that create, amend or cancel real orders across every adapter
# the project ships (Binance, Bybit, BingX, OANDA, IBKR, MT bridge).
_ORDER_PATH_FRAGMENTS = (
    "/order",
    "/orders",
    "/batchorders",
    "/allopenorders",
    "/v5/order",
    "/openapi/swap",
    "/iserver/account",
    "/trade/order",
)


def _is_order_endpoint(url: str) -> bool:
    lowered = str(url or "").lower().split("?", 1)[0]
    return any(fragment in lowered for fragment in _ORDER_PATH_FRAGMENTS)


@pytest.fixture(autouse=True)
def block_live_order_submission(monkeypatch, request):
    """Fail any test that tries to POST/PUT/DELETE a broker order endpoint.

    Read-only market-data calls are untouched so connectivity tests still work.
    A test that genuinely needs to exercise the transport can opt out with
    ``@pytest.mark.allow_live_orders``.
    """
    if request.node.get_closest_marker("allow_live_orders"):
        yield
        return

    import requests

    original_request = requests.Session.request

    def guarded_request(self, method, url, *args, **kwargs):
        if str(method).upper() in {"POST", "PUT", "DELETE"} and _is_order_endpoint(url):
            raise LiveOrderSubmissionBlocked(
                f"Test attempted a live order call: {method} {url}. "
                "Paper execution must stay inside PaperExecutor."
            )
        return original_request(self, method, url, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "request", guarded_request)

    for verb in ("post", "put", "delete"):
        original_verb = getattr(requests.Session, verb)

        def make_guard(_verb, _original):
            def guarded(self, url, *args, **kwargs):
                if _is_order_endpoint(url):
                    raise LiveOrderSubmissionBlocked(
                        f"Test attempted a live order call: {_verb.upper()} {url}."
                    )
                return _original(self, url, *args, **kwargs)

            return guarded

        monkeypatch.setattr(requests.Session, verb, make_guard(verb, original_verb))

    yield


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "allow_live_orders: opt out of the live-order transport guard (transport tests only)",
    )
