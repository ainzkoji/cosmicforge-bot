"""Test-database isolation: the one place that decides whether a test may open a database.

Found by auditing the canonical paper database while the live runtime was up:
9,339 rows in ``canonical_trade_decisions`` and 9,339 in ``decision_traces``
belonged to test bots (``bot_replay_*``, ``bot_determinism_*``,
``bot_sensitivity``), 849 of them written *during* a live paper session by a
concurrent ``pytest`` run. Two mechanisms put them there:

* ``app.main`` loads ``.env`` with ``override=True`` at import, so the first
  test that imported the app replaced any test ``DATABASE_URL`` with the
  canonical one for the rest of the session;
* ``get_trace_recorder()`` cached a process-wide recorder bound to whatever
  ``DB().path`` resolved to first, and kept writing there even after a test
  pointed ``DATABASE_URL`` somewhere else.

Isolation is therefore enforced where a database is *opened*, not where a test
is written. :func:`enforce_test_database` is called by ``DB()`` and by
``TraceRecorder``; under ``COSMICFORGE_TEST_MODE=1`` it refuses, by raising, any
database that is -- or might be -- a runtime database. Refusing is the point:
a warning would be read after the rows were already written.

This module deliberately imports nothing from ``app`` so the pytest bootstrap
can use it before any application import happens.
"""
from __future__ import annotations

import os
from pathlib import Path

TEST_MODE_ENV = "COSMICFORGE_TEST_MODE"

#: Database roles a test process may never open, whatever the file is called.
PROTECTED_ROLES = frozenset({"paper", "live", "production", "prod", "mainnet"})

#: File names that identify the canonical runtime database wherever it lives.
PROTECTED_BASENAMES = frozenset({"cosmicforge.db"})

#: ``backends/shared/shared_lib/persistence`` -- the canonical database's home.
#: Every ``*.db`` file here is a runtime, forensic, backup or validation copy.
PERSISTENCE_DIR = Path(__file__).resolve().parent
BACKENDS_DIR = PERSISTENCE_DIR.parents[2]
BOT_BACKEND_ENV = BACKENDS_DIR / "bot-backend" / ".env"


class TestDatabaseIsolationError(RuntimeError):
    """A test process tried to open a runtime database."""

    code = "TEST_DATABASE_ISOLATION_VIOLATION"

    # Not a test class, whatever pytest's name-based collection thinks.
    __test__ = False


def test_mode_active() -> bool:
    return os.environ.get(TEST_MODE_ENV) == "1"


# ``test_mode_active`` is a predicate, not a test.
test_mode_active.__test__ = False  # type: ignore[attr-defined]


def resolve_sqlite_url(url: str | None) -> str | None:
    """Resolve a ``sqlite:///`` URL exactly as ``DB()`` does. ``None`` otherwise."""
    if not url or not str(url).startswith("sqlite:///"):
        return None
    rel = str(url)[len("sqlite:///"):]
    if rel == ":memory:":
        return rel
    if not os.path.isabs(rel):
        return os.path.abspath(os.path.join(str(BACKENDS_DIR), "bot-backend", rel))
    return rel


def configured_runtime_database(env_file: Path = BOT_BACKEND_ENV) -> str | None:
    """The database the runtime ``.env`` points at, read from the file itself.

    Read from disk rather than from ``os.environ`` because the test bootstrap
    has already replaced ``DATABASE_URL`` in the environment; this is the value
    the environment is protecting.
    """
    try:
        text = Path(env_file).read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        if key.strip() == "DATABASE_URL":
            return resolve_sqlite_url(value.strip().strip('"').strip("'"))
    return None


def _norm(path: str | os.PathLike) -> str:
    return os.path.normcase(os.path.abspath(str(path)))


def protection_reason(path: str | None, *, role: str | None = None) -> str | None:
    """Why ``path`` (or ``role``) must not be opened by a test. ``None`` if it may."""
    if role and str(role).strip().lower() in PROTECTED_ROLES:
        return f"DATABASE_ROLE={role!r} is a runtime role"
    if not path:
        return None
    text = str(path)
    if text == ":memory:" or text.startswith("file:"):
        return None
    candidate = Path(text)
    if candidate.name.lower() in PROTECTED_BASENAMES:
        return f"{candidate.name} is the canonical runtime database name"
    if (
        candidate.suffix.lower() == ".db"
        and _norm(candidate.parent) == _norm(PERSISTENCE_DIR)
    ):
        return "it lives in the canonical persistence directory"
    runtime = configured_runtime_database()
    if runtime and runtime != ":memory:" and _norm(candidate) == _norm(runtime):
        return "it is the DATABASE_URL configured in bot-backend/.env"
    return None


def enforce_test_database(path: str | None, *, role: str | None = None) -> None:
    """Raise if a test process is about to open a runtime database.

    A no-op outside test mode: the runtime opens the canonical database by
    design, and this guard has no opinion about that.
    """
    if not test_mode_active():
        return
    effective_role = role if role is not None else os.environ.get("DATABASE_ROLE")
    reason = protection_reason(path, role=effective_role)
    if reason:
        raise TestDatabaseIsolationError(
            f"[{TestDatabaseIsolationError.code}] test mode refused to open "
            f"database {path!r}: {reason}. Tests run against the per-session "
            f"temporary database the pytest bootstrap creates; they never touch "
            f"paper, live or canonical evidence."
        )


__all__ = [
    "BACKENDS_DIR",
    "BOT_BACKEND_ENV",
    "PERSISTENCE_DIR",
    "PROTECTED_BASENAMES",
    "PROTECTED_ROLES",
    "TEST_MODE_ENV",
    "TestDatabaseIsolationError",
    "configured_runtime_database",
    "enforce_test_database",
    "protection_reason",
    "resolve_sqlite_url",
    "test_mode_active",
]
