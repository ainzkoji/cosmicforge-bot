"""Every platform-layer test runs on SQLite and, when a server is available, on PostgreSQL.

``PLATFORM_TEST_POSTGRES_URL`` names a server where the tests may create and drop throwaway databases (CI uses a
service container; locally a disposable ``initdb`` cluster). Without it the PostgreSQL half is skipped -- unless
``PLATFORM_TEST_POSTGRES_REQUIRED=1`` (set in CI), which turns the skip into a failure so the PostgreSQL evidence
cannot silently disappear.
"""
from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from shared_lib.persistence.sql import Database, migrate  # noqa: E402
from shared_lib.persistence.sql.platform_schema import all_migrations  # noqa: E402

from _pg_server import PG_REQUIRED, PG_URL, _pg_admin, _pg_url  # noqa: E402,F401


@pytest.fixture(params=["sqlite", "postgresql"])
def database_url(request, tmp_path):
    """A URL to an empty, throwaway database of each dialect."""
    if request.param == "sqlite":
        yield f"sqlite:///{(tmp_path / 'platform.db').as_posix()}"
        return
    if not PG_URL:
        if PG_REQUIRED:
            pytest.fail("PLATFORM_TEST_POSTGRES_REQUIRED=1 but PLATFORM_TEST_POSTGRES_URL is not set")
        pytest.skip("PLATFORM_TEST_POSTGRES_URL not set")
    name = f"cfpt_{uuid.uuid4().hex[:12]}"
    _pg_admin(f"CREATE DATABASE {name}")
    try:
        yield _pg_url(name)
    finally:
        _pg_admin(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


@pytest.fixture
def platform_db(database_url):
    db = Database(database_url, pool_size=16)
    migrate(db, all_migrations())
    yield db
    db.close()
