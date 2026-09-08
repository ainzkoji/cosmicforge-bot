"""Import bootstrap for the Phase 12 harness.

Must run before anything imports ``shared_lib.persistence.db``: ``DB()``
resolves its path from ``DATABASE_URL`` at construction, and the canonical
``.env`` points at the production runtime database. The harness runs against a
dedicated validation database with the identical schema, so a synthetic OPEN
position can never appear in the production evidence, position counts or
operational health.
"""
from __future__ import annotations

import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BACKEND_DIR = os.path.join(REPO_ROOT, "backends", "bot-backend")
SHARED_DIR = os.path.join(REPO_ROOT, "backends", "shared")

#: Beside the canonical database, with a name no operator could mistake for it.
DEFAULT_VALIDATION_DB = os.path.join(
    REPO_ROOT, "backends", "shared", "shared_lib", "persistence",
    "phase12_paper_validation.db",
)

CANONICAL_DB = os.path.join(
    REPO_ROOT, "backends", "shared", "shared_lib", "persistence", "cosmicforge.db",
)

#: Never touched by this harness. Asserted, not merely documented.
FORBIDDEN_BOTS = ("bot_e5fe913972a9", "bot_a8117dc719fc")


def bootstrap(db_path: str | None = None) -> str:
    """Put the backend on the path and pin the validation database.

    Returns the absolute database path actually in force.
    """
    for path in (BACKEND_DIR, SHARED_DIR):
        if path not in sys.path:
            sys.path.insert(0, path)

    # The backend resolves relative paths and config from its own directory.
    os.chdir(BACKEND_DIR)

    env_path = os.path.join(BACKEND_DIR, ".env")
    if os.path.exists(env_path):
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env_path, override=True)

    resolved = os.path.abspath(db_path or DEFAULT_VALIDATION_DB)
    if os.path.abspath(resolved) == os.path.abspath(CANONICAL_DB):
        raise SystemExit(
            "refusing to run the Phase 12 harness against the canonical runtime "
            "database. A synthetic position there would contaminate operational "
            "health, position counts and readiness evidence."
        )

    # DB() reads DATABASE_URL and load_dotenv() does not override an existing
    # environment variable, so setting it here wins over the backend .env.
    os.environ["DATABASE_URL"] = "sqlite:///" + resolved.replace("\\", "/")
    os.environ["EXECUTION_MODE"] = "paper"
    os.environ.setdefault("PAPER_TRADING_MODE", "true")

    return resolved


def assert_database_is_validation(db) -> None:
    """Fail closed if anything re-resolved the DB back to the canonical file."""
    actual = os.path.abspath(getattr(db, "path", "") or "")
    if actual == os.path.abspath(CANONICAL_DB):
        raise SystemExit(f"Phase 12 harness resolved the canonical database: {actual}")
