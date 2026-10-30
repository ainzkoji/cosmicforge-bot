"""The throwaway PostgreSQL server the platform tests may use (``PLATFORM_TEST_POSTGRES_URL``).

Kept in its own module so test files can import these names directly: ``from conftest import ...`` resolves to
whichever ``conftest`` pytest loaded last, which is another directory's when several are collected together.
"""
from __future__ import annotations

import os

PG_URL = os.environ.get("PLATFORM_TEST_POSTGRES_URL", "")
PG_REQUIRED = os.environ.get("PLATFORM_TEST_POSTGRES_REQUIRED") == "1"


def _pg_admin(sql: str) -> None:
    import psycopg
    with psycopg.connect(PG_URL, autocommit=True) as conn:
        conn.execute(sql)


def _pg_url(name: str) -> str:
    base, _, _ = PG_URL.rpartition("/")
    return f"{base}/{name}"
